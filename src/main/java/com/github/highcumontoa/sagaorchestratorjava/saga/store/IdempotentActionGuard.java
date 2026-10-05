package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 动作级幂等守卫。
 *
 * <p>键为 {@code 实例 + 步骤 + 阶段(FORWARD/COMPENSATE)}，状态有三种：
 * <ul>
 *   <li>{@link #IN_FLIGHT}：动作已开始但结果尚未确认（例如超时、JVM 崩溃）。
 *       这类记录不代表副作用已生效，再次 begin 时允许重放；</li>
 *   <li>{@link #DONE}：动作已确认成功，副作用已生效。再次 begin 直接短路，绝不重复执行；</li>
 *   <li>{@link #FAILED}：动作已确认失败，允许下一次重试重新 begin。</li>
 * </ul>
 *
 * <p>为处理“超时后旧尝试迟到回调”，每次 begin 带单调递增的 attempt（fencing token），
 * complete/fail 时必须与当前 attempt 一致才接受；旧尝试的迟到结果被丢弃。
 *
 * <p>记录落盘，重启后守卫依然有效；同一键在进程内加锁，避免并发线程同时穿透。
 */
public class IdempotentActionGuard {

    public enum Outcome {
        /** 本次获得执行权（新建或重放 IN_FLIGHT/FAILED）。 */
        NEW_EXECUTION,
        /** 此前已 DONE，必须跳过真实执行，可直接复用 resultJson。 */
        ALREADY_DONE
    }

    /** begin 的判定结论，携带判定依据。 */
    public record BeginResult(Outcome outcome, String previousState, int previousAttempt, String resultJson) {
    }

    public static final String IN_FLIGHT = "IN_FLIGHT";
    public static final String DONE = "DONE";
    public static final String FAILED = "FAILED";

    private static final Logger log = LoggerFactory.getLogger(IdempotentActionGuard.class);

    private final SagaStore store;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public IdempotentActionGuard(SagaStore store) {
        this.store = store;
    }

    public static String key(String instanceId, String stepName, String phase) {
        return instanceId + "|" + stepName + "|" + phase;
    }

    /**
     * 只读查看某个键的守卫状态（供快照/轨迹解释在途中间态），不改变任何状态。
     *
     * @return 守卫记录；从未下发过动作时返回 empty
     */
    public Optional<GuardRecord> inspect(String key) {
        return store.findGuard(key);
    }

    /**
     * 尝试开始一次受守卫保护的动作。
     *
     * @param attempt 本次尝试序号（从 1 开始，单调递增）
     * @return ALREADY_DONE 表示此前已确认成功，调用方不得再次执行业务动作；
     *         NEW_EXECUTION 表示可以执行
     */
    public BeginResult begin(String key, int attempt) {
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Optional<GuardRecord> existing = store.findGuard(key);
            if (existing.isPresent() && DONE.equals(existing.get().getState())) {
                GuardRecord done = existing.get();
                log.info("[guard] key={} already DONE(attempt={}) -> suppress duplicate side effect",
                        key, done.getAttempt());
                return new BeginResult(Outcome.ALREADY_DONE, DONE, done.getAttempt(), done.getResultJson());
            }
            String previousState = existing.map(GuardRecord::getState).orElse("ABSENT");
            int previousAttempt = existing.map(GuardRecord::getAttempt).orElse(0);
            GuardRecord record = existing.orElseGet(GuardRecord::new);
            record.setKey(key);
            record.setState(IN_FLIGHT);
            record.setAttempt(attempt);
            record.setUpdatedMillis(System.currentTimeMillis());
            store.saveGuard(record);
            log.info("[guard] key={} begin attempt={} (previous={}/{}) -> allow execution",
                    key, attempt, previousState, previousAttempt);
            return new BeginResult(Outcome.NEW_EXECUTION, previousState, previousAttempt, null);
        } finally {
            lock.unlock();
        }
    }

    /** 仅当 attempt 与当前守卫记录一致时接受成功回调，丢弃迟到的旧尝试结果。 */
    public boolean complete(String key, int attempt, String resultJson) {
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            GuardRecord record = store.findGuard(key).orElse(null);
            if (record == null || record.getAttempt() != attempt || !IN_FLIGHT.equals(record.getState())) {
                log.warn("[guard] key={} stale success callback attempt={} ignored (current={}/{})",
                        key, attempt,
                        record == null ? "ABSENT" : record.getState(),
                        record == null ? -1 : record.getAttempt());
                return false;
            }
            record.setState(DONE);
            record.setResultJson(resultJson);
            record.setError(null);
            record.setUpdatedMillis(System.currentTimeMillis());
            store.saveGuard(record);
            log.info("[guard] key={} attempt={} -> DONE", key, attempt);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 仅当 attempt 与当前守卫记录一致时接受失败回调。 */
    public boolean fail(String key, int attempt, String error) {
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            GuardRecord record = store.findGuard(key).orElse(null);
            if (record == null || record.getAttempt() != attempt || !IN_FLIGHT.equals(record.getState())) {
                log.warn("[guard] key={} stale failure callback attempt={} ignored (current={}/{})",
                        key, attempt,
                        record == null ? "ABSENT" : record.getState(),
                        record == null ? -1 : record.getAttempt());
                return false;
            }
            record.setState(FAILED);
            record.setError(error);
            record.setUpdatedMillis(System.currentTimeMillis());
            store.saveGuard(record);
            log.info("[guard] key={} attempt={} -> FAILED ({})", key, attempt, error);
            return true;
        } finally {
            lock.unlock();
        }
    }
}
