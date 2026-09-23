package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.ActionContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.Compensation;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaStep;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.IllegalSagaTransitionException;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaConcurrentConflictException;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaNotFoundException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.AttemptOutcome;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.RetryPolicy;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepAttempt;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepPolicies;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepState;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceEvent;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.SagaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 编排器：提交实例、推进正向执行与补偿、查询状态与轨迹。
 *
 * <p>核心保证：
 * <ul>
 *   <li>同一实例 ID 的重复/并发提交收敛到唯一实例（后来者等待先行者完成，不重复执行）；</li>
 *   <li>所有状态推进基于存储层 CAS（乐观版本）+ 进程内按实例互斥锁；CAS 冲突有限重试后拒绝；</li>
 *   <li>每次尝试先持久化“在途标记”（outcome=null + executionId）再调用动作，
 *       崩溃恢复时用同一 executionId 复查，已完成步骤永不重复执行；</li>
 *   <li>失败后已成功步骤按逆序补偿，补偿失败保留中间状态（待补偿/补偿中/补偿失败可区分），
 *       重复补偿幂等。</li>
 * </ul>
 */
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);
    private static final int LOCK_STRIPES = 64;

    private final SagaRepository repository;
    private final SagaDefinitionRegistry registry;
    private final AttemptRunner attemptRunner;
    private final ReentrantLock[] instanceLocks;

    /**
     * 测试缝隙：在“在途尝试标记”落盘之后、动作结论落盘之前触发。
     * 抛出异常即模拟进程此刻崩溃（标记已持久化，结论未知）。生产中始终为 null。
     */
    private volatile InFlightCrashHook inFlightCrashHook;

    /** 见 {@link #inFlightCrashHook}。 */
    public interface InFlightCrashHook {
        void afterDispatchPersisted(SagaInstance instance, StepState step, String kind, int attemptNo);
    }

    public void setInFlightCrashHook(InFlightCrashHook hook) {
        this.inFlightCrashHook = hook;
    }

    public SagaOrchestrator(SagaRepository repository, SagaDefinitionRegistry registry) {
        this(repository, registry, new AttemptRunner());
    }

    SagaOrchestrator(SagaRepository repository, SagaDefinitionRegistry registry, AttemptRunner attemptRunner) {
        this.repository = repository;
        this.registry = registry;
        this.attemptRunner = attemptRunner;
        this.instanceLocks = new ReentrantLock[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) {
            this.instanceLocks[i] = new ReentrantLock();
        }
    }

    private ReentrantLock lockFor(String instanceId) {
        return instanceLocks[Math.floorMod(instanceId.hashCode(), LOCK_STRIPES)];
    }

    /**
     * 提交（或幂等获取）一个实例并推进正向执行。
     *
     * @return 提交/推进后的实例；重复提交同 instanceId 时不重复执行，直接返回既有（已收敛）实例
     */
    public SagaInstance submit(SagaDefinition definition, String instanceId, SagaContext initialContext) {
        registry.register(definition);
        ReentrantLock lock = lockFor(instanceId);
        lock.lock();
        try {
            SagaInstance existing = repository.find(instanceId).orElse(null);
            if (existing != null) {
                log.info("[SUBMIT-DUP] instanceId={} currentStatus={} -> 幂等返回既有实例，不重复执行",
                        instanceId, existing.getStatus());
                return existing;
            }
            SagaInstance instance = newInstance(definition, instanceId, initialContext);
            if (!repository.insertIfAbsent(instance)) {
                // 与存储中其他进程/线程的插入竞争落败，收敛到既有实例
                SagaInstance concurrent = requireInstance(instanceId);
                log.info("[SUBMIT-CONFLICT] instanceId={} 插入落败，收敛到既有实例 status={}",
                        instanceId, concurrent.getStatus());
                return concurrent;
            }
            log.info("[SUBMIT] instanceId={} definition={} steps={} 已创建并开始正向执行",
                    instanceId, definition.name(), definition.steps().size());
            return drive(instance);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 幂等触发补偿。
     * <ul>
     *   <li>COMPLETED：转为 COMPENSATING 并按逆序补偿全部成功步骤；</li>
     *   <li>COMPENSATION_FAILED：继续推进剩余/失败的补偿（幂等重试）；</li>
     *   <li>COMPENSATED：直接返回，补偿不重复执行；</li>
     *   <li>RUNNING / COMPENSATING：以 ILLEGAL_TRANSITION 明确拒绝。</li>
     * </ul>
     */
    public SagaInstance compensate(String instanceId) {
        ReentrantLock lock = lockFor(instanceId);
        lock.lock();
        try {
            SagaInstance instance = requireInstance(instanceId);
            SagaDefinition definition = requireDefinition(instance);
            switch (instance.getStatus()) {
                case COMPLETED -> {
                    markCompensationPending(instance, "用户/策略显式触发补偿，全部已成功步骤进入待补偿");
                    instance.setStatus(InstanceStatus.COMPENSATING);
                    instance.addTrace("MANUAL_COMPENSATE", "COMPLETED -> COMPENSATING，逆序补偿开始");
                    log.info("[COMPENSATE] instanceId={} COMPLETED -> COMPENSATING", instanceId);
                    instance = save(instance);
                    return driveCompensation(instance, definition);
                }
                case COMPENSATION_FAILED -> {
                    log.info("[COMPENSATE-RETRY] instanceId={} 继续补偿（幂等重试）", instanceId);
                    instance.addTrace("MANUAL_COMPENSATE", "COMPENSATION_FAILED -> COMPENSATING，重试剩余补偿");
                    instance.setStatus(InstanceStatus.COMPENSATING);
                    instance = save(instance);
                    return driveCompensation(instance, definition);
                }
                case COMPENSATED -> {
                    // 终态幂等：纯只读空操作，不产生新版本、不重复补偿
                    log.info("[COMPENSATE-IDEMPOTENT] instanceId={} 已补偿完成，重复触发直接收敛", instanceId);
                    return instance;
                }
                default -> throw new IllegalSagaTransitionException(
                        "cannot trigger compensation while instance is " + instance.getStatus()
                                + " (instanceId=" + instanceId + ")");
            }
        } finally {
            lock.unlock();
        }
    }

    /** 查询实例；未知实例以 UNKNOWN_INSTANCE 拒绝。 */
    public SagaInstance get(String instanceId) {
        return requireInstance(instanceId);
    }

    /** 查询执行轨迹；未知实例以 UNKNOWN_INSTANCE 拒绝。 */
    public List<TraceEvent> getTrace(String instanceId) {
        return List.copyOf(requireInstance(instanceId).getTrace());
    }

    /**
     * 组件重启/重新初始化后继续推进所有未完成实例。
     * 不触碰终态实例（COMPLETED 不重复执行、COMPENSATED 不重复补偿）。
     *
     * @return 被恢复推进的实例数
     */
    public int recoverAll() {
        List<SagaInstance> all = repository.findAll();
        int recovered = 0;
        for (SagaInstance instance : all) {
            if (isTerminal(instance.getStatus())) {
                continue;
            }
            ReentrantLock lock = lockFor(instance.getInstanceId());
            lock.lock();
            try {
                SagaInstance fresh = requireInstance(instance.getInstanceId());
                if (isTerminal(fresh.getStatus())) {
                    continue;
                }
                SagaDefinition definition = requireDefinition(fresh);
                log.info("[RECOVER] instanceId={} status={} 重新接管推进", fresh.getInstanceId(), fresh.getStatus());
                fresh.addTrace("RECOVERED", "组件恢复：以状态 " + fresh.getStatus()
                        + " 继续推进（已确认完成的步骤不重复执行）");
                fresh = save(fresh);
                if (fresh.getStatus() == InstanceStatus.RUNNING) {
                    drive(fresh);
                } else {
                    driveCompensation(fresh, definition);
                }
                recovered++;
            } finally {
                lock.unlock();
            }
        }
        log.info("[RECOVER] 恢复完成，推进实例数={}", recovered);
        return recovered;
    }

    // ---------------- 正向执行 ----------------

    private SagaInstance drive(SagaInstance instance) {
        SagaDefinition definition = requireDefinition(instance);
        List<SagaStep> steps = definition.steps();
        int i = firstForwardIndex(instance);
        while (i < steps.size()) {
            StepState step = instance.getSteps().get(i);
            SagaStep sagaStep = steps.get(i);
            if (step.getStatus() == StepStatus.SUCCEEDED) {
                i++;
                continue;
            }
            if (step.getStatus() == StepStatus.RUNNING) {
                // 恢复接管：结论未知的在途尝试必须存在标记，由 executeForward 复用其 executionId 复查
                StepAttempt last = step.getForwardAttempts().isEmpty()
                        ? null : step.getForwardAttempts().get(step.getForwardAttempts().size() - 1);
                if (last == null || !last.inDoubt()) {
                    throw new IllegalStateException(
                            "step RUNNING without in-doubt marker: " + step.getName());
                }
            }
            // PENDING / RUNNING(在途复查) / FAILED(重试点) 均在此推进
            boolean success = executeForward(instance, sagaStep, step);
            if (!success) {
                enterCompensation(instance, "正向步骤[" + step.getName() + "]失败，开始逆序补偿");
                return driveCompensation(instance, definition);
            }
            i++;
        }
        instance.setStatus(InstanceStatus.COMPLETED);
        instance.addTrace("STATE_TRANSITION", "全部步骤成功 -> COMPLETED");
        log.info("[COMPLETE] instanceId={} 正向执行完成", instance.getInstanceId());
        return save(instance);
    }

    private int firstForwardIndex(SagaInstance instance) {
        int i = 0;
        while (i < instance.getSteps().size()
                && instance.getSteps().get(i).getStatus() == StepStatus.SUCCEEDED) {
            i++;
        }
        return i;
    }

    private boolean executeForward(SagaInstance instance, SagaStep sagaStep, StepState step) {
        StepPolicies policies = sagaStep.policies();
        RetryPolicy retry = policies.forwardRetry();
        // 崩溃恢复时可能已有 1..maxAttempts 的确定记录；新驱动轮次从其后继续编号。
        int baseNo = maxFinishedAttemptNo(step.getForwardAttempts(), StepAttempt.FORWARD);

        for (int n = 1; n <= retry.maxAttempts(); n++) {
            int attemptNo = baseNo + n;
            StepAttempt existing = findAttempt(step.getForwardAttempts(), StepAttempt.FORWARD, attemptNo);
            boolean replay = existing != null && existing.inDoubt();
            String executionId = existing != null
                    ? existing.executionId()
                    : newExecutionId(instance, step, StepAttempt.FORWARD, attemptNo);

            if (existing == null) {
                if (n > 1) {
                    sleepBackoff(retry.backoff(), instance.getInstanceId(), step.getName());
                }
                step.getForwardAttempts().add(new StepAttempt(StepAttempt.FORWARD, attemptNo, executionId,
                        null, null, Instant.now(), null));
            }
            step.setStatus(StepStatus.RUNNING);
            instance.addTrace("ATTEMPT_DISPATCH",
                    "步骤[" + step.getName() + "]正向尝试#" + attemptNo
                            + " executionId=" + executionId
                            + (replay ? "（崩溃恢复，复用同一执行ID复查）" : "已下发"));
            log.info("[FORWARD-DISPATCH] instanceId={} step={} attempt#{} executionId={} replay={}",
                    instance.getInstanceId(), step.getName(), attemptNo, executionId, replay);
            instance = save(instance);
            crashAfterDispatchIfArmed(instance, step, StepAttempt.FORWARD, attemptNo);

            ActionContext ctx = new ActionContext(instance.getInstanceId(), step.getName(), executionId,
                    attemptNo, instance.getContext(), replay);
            AttemptResult result = attemptRunner.run(() -> sagaStep.action().execute(ctx),
                    policies.forwardTimeout(), executionId);

            finishAttempt(step.getForwardAttempts(), StepAttempt.FORWARD, attemptNo, result);
            if (result.outcome() == AttemptOutcome.SUCCESS) {
                step.setStatus(StepStatus.SUCCEEDED);
                instance.setCurrentStepIndex(step.getIndex() + 1);
                instance.addTrace("ATTEMPT_RESULT", "步骤[" + step.getName() + "]正向成功 -> SUCCEEDED，补偿已登记");
                log.info("[FORWARD-OK] instanceId={} step={} attempt#{} 判定依据={}",
                        instance.getInstanceId(), step.getName(), attemptNo, result.outcome());
                instance = save(instance);
                return true;
            }
            instance.addTrace("ATTEMPT_RESULT", "步骤[" + step.getName() + "]正向尝试#" + attemptNo
                    + " 结论=" + result.outcome() + " 原因=" + result.error());
            log.warn("[FORWARD-FAIL] instanceId={} step={} attempt#{} outcome={} error={}",
                    instance.getInstanceId(), step.getName(), attemptNo, result.outcome(), result.error());
            instance = save(instance);
        }
        // 重试耗尽
        step.setStatus(StepStatus.FAILED);
        instance.setLastFailureReason("forward step [" + step.getName() + "] failed after "
                + retry.maxAttempts() + " attempts");
        instance.addTrace("ATTEMPT_RESULT", "步骤[" + step.getName() + "]重试耗尽 -> FAILED");
        log.warn("[FORWARD-EXHAUSTED] instanceId={} step={} 判定依据=重试已耗尽", instance.getInstanceId(), step.getName());
        return false;
    }


    // ---------------- 补偿执行 ----------------

    private void enterCompensation(SagaInstance instance, String reason) {
        markCompensationPending(instance, reason);
        instance.setStatus(InstanceStatus.COMPENSATING);
    }

    /** 将所有正向成功步骤标记为待补偿（失败步骤本身及之后步骤不补偿）。 */
    private void markCompensationPending(SagaInstance instance, String reason) {
        for (StepState step : instance.getSteps()) {
            if (step.getStatus() == StepStatus.SUCCEEDED) {
                step.setStatus(StepStatus.COMPENSATION_PENDING);
                instance.addTrace("STATE_TRANSITION",
                        "步骤[" + step.getName() + "] SUCCEEDED -> COMPENSATION_PENDING（待补偿）");
            }
        }
        instance.addTrace("STATE_TRANSITION", reason);
    }

    private SagaInstance driveCompensation(SagaInstance instance, SagaDefinition definition) {
        List<SagaStep> steps = definition.steps();
        // 从最高位的成功步骤开始逆序；FAILED 步骤本身没有产生效果，不补偿
        int i = lastCompensableIndex(instance);
        while (i >= 0) {
            StepState step = instance.getSteps().get(i);
            SagaStep sagaStep = steps.get(i);
            if (step.getStatus() == StepStatus.COMPENSATED) {
                i--;
                continue;
            }
            if (step.getStatus() == StepStatus.SUCCEEDED) {
                // 恢复场景中可能尚未改写状态
                step.setStatus(StepStatus.COMPENSATION_PENDING);
            }
            if (step.getStatus() != StepStatus.COMPENSATION_PENDING
                    && step.getStatus() != StepStatus.COMPENSATING
                    && step.getStatus() != StepStatus.COMPENSATION_FAILED) {
                // 非补偿态步骤（PENDING/FAILED/RUNNING）未产生已确认效果，绝不补偿
                log.warn("[COMPENSATION-SKIP] instanceId={} step={} status={} 非补偿态，跳过",
                        instance.getInstanceId(), step.getName(), step.getStatus());
                i--;
                continue;
            }
            if (sagaStep.compensation() == null) {
                // 无补偿动作：直接视为补偿完成
                step.setStatus(StepStatus.COMPENSATED);
                instance.addTrace("STATE_TRANSITION", "步骤[" + step.getName() + "]无补偿动作，直接标记 COMPENSATED");
                instance = save(instance);
                i--;
                continue;
            }
            boolean success = executeCompensation(instance, sagaStep, step);
            if (!success) {
                instance.setStatus(InstanceStatus.COMPENSATION_FAILED);
                instance.setLastFailureReason("compensation step [" + step.getName() + "] failed");
                instance.addTrace("STATE_TRANSITION",
                        "实例 -> COMPENSATION_FAILED：步骤[" + step.getName() + "]补偿失败，中间状态保留，可幂等重试");
                log.warn("[COMPENSATION-FAILED] instanceId={} failedStep={} 更早的步骤保持待补偿",
                        instance.getInstanceId(), step.getName());
                return save(instance);
            }
            i--;
        }
        instance.setStatus(InstanceStatus.COMPENSATED);
        instance.addTrace("STATE_TRANSITION", "全部补偿完成 -> COMPENSATED（终态）");
        log.info("[COMPENSATED] instanceId={} 逆序补偿全部完成", instance.getInstanceId());
        return save(instance);
    }

    private int lastCompensableIndex(SagaInstance instance) {
        for (int i = instance.getSteps().size() - 1; i >= 0; i--) {
            StepStatus s = instance.getSteps().get(i).getStatus();
            if (s == StepStatus.SUCCEEDED || s == StepStatus.COMPENSATION_PENDING
                    || s == StepStatus.COMPENSATING || s == StepStatus.COMPENSATION_FAILED
                    || s == StepStatus.COMPENSATED) {
                return i;
            }
        }
        return -1;
    }

    private boolean executeCompensation(SagaInstance instance, SagaStep sagaStep, StepState step) {
        StepPolicies policies = sagaStep.policies();
        RetryPolicy retry = policies.compensationRetry();

        // 进入补偿中（补偿失败重试时：COMPENSATION_FAILED -> COMPENSATING）
        step.setStatus(StepStatus.COMPENSATING);
        // 重复触发补偿（COMPENSATION_FAILED 后重试）：历史确定失败的尝试保留轨迹，
        // 新一轮尝试获得全新 attemptNo/executionId；只有在途标记才复用复查。
        int baseNo = maxFinishedAttemptNo(step.getCompensationAttempts(), StepAttempt.COMPENSATE);
        for (int n = 1; n <= retry.maxAttempts(); n++) {
            int attemptNo = baseNo + n;
            StepAttempt existing = findAttempt(step.getCompensationAttempts(), StepAttempt.COMPENSATE, attemptNo);
            boolean replay = existing != null && existing.inDoubt();
            String executionId = existing != null
                    ? existing.executionId()
                    : newExecutionId(instance, step, StepAttempt.COMPENSATE, attemptNo);

            if (existing == null) {
                if (n > 1) {
                    sleepBackoff(retry.backoff(), instance.getInstanceId(), step.getName());
                }
                step.getCompensationAttempts().add(new StepAttempt(StepAttempt.COMPENSATE, attemptNo, executionId,
                        null, null, Instant.now(), null));
            }
            instance.addTrace("ATTEMPT_DISPATCH",
                    "步骤[" + step.getName() + "]补偿尝试#" + attemptNo
                            + " executionId=" + executionId
                            + (replay ? "（崩溃恢复，复用同一执行ID复查）" : "已下发"));
            log.info("[COMPENSATE-DISPATCH] instanceId={} step={} attempt#{} executionId={} replay={}",
                    instance.getInstanceId(), step.getName(), attemptNo, executionId, replay);
            instance = save(instance);
            crashAfterDispatchIfArmed(instance, step, StepAttempt.COMPENSATE, attemptNo);

            ActionContext ctx = new ActionContext(instance.getInstanceId(), step.getName(), executionId,
                    attemptNo, instance.getContext(), replay);
            Compensation compensation = sagaStep.compensation();
            AttemptResult result = attemptRunner.run(() -> compensation.compensate(ctx),
                    policies.compensationTimeout(), executionId);

            finishAttempt(step.getCompensationAttempts(), StepAttempt.COMPENSATE, attemptNo, result);
            if (result.outcome() == AttemptOutcome.SUCCESS) {
                step.setStatus(StepStatus.COMPENSATED);
                instance.addTrace("ATTEMPT_RESULT", "步骤[" + step.getName() + "]补偿成功 -> COMPENSATED");
                log.info("[COMPENSATE-OK] instanceId={} step={} attempt#{}",
                        instance.getInstanceId(), step.getName(), attemptNo);
                instance = save(instance);
                return true;
            }
            instance.addTrace("ATTEMPT_RESULT", "步骤[" + step.getName() + "]补偿尝试#" + attemptNo
                    + " 结论=" + result.outcome() + " 原因=" + result.error());
            log.warn("[COMPENSATE-FAIL] instanceId={} step={} attempt#{} outcome={} error={}",
                    instance.getInstanceId(), step.getName(), attemptNo, result.outcome(), result.error());
            instance = save(instance);
        }
        step.setStatus(StepStatus.COMPENSATION_FAILED);
        log.warn("[COMPENSATE-EXHAUSTED] instanceId={} step={} 判定依据=补偿重试已耗尽",
                instance.getInstanceId(), step.getName());
        return false;
    }


    // ---------------- 持久化辅助 ----------------

    /** 测试缝隙：模拟“在途标记已落盘、结论未知时崩溃”。 */
    private void crashAfterDispatchIfArmed(SagaInstance instance, StepState step, String kind, int attemptNo) {
        InFlightCrashHook hook = inFlightCrashHook;
        if (hook != null) {
            hook.afterDispatchPersisted(instance, step, kind, attemptNo);
        }
    }

    /**
     * CAS 落盘：以内存对象当前版本为期望版本更新，成功后存储层将版本 +1。
     *
     * <p>本进程内同一实例的所有推进都串行在按实例互斥锁内，正常不会冲突；
     * 一旦版本不一致（如另一进程/外部已改写存储），直接以 CONCURRENT_CONFLICT 拒绝，
     * 绝不静默覆盖。
     */
    private SagaInstance save(SagaInstance instance) {
        instance.setUpdatedAt(Instant.now());
        try {
            repository.compareAndUpdate(instance, instance.getVersion());
            return instance;
        } catch (SagaConcurrentConflictException e) {
            log.error("[CAS-REJECT] instanceId={} version={} 存储版本已被改写，拒绝原因=CONCURRENT_CONFLICT",
                    instance.getInstanceId(), instance.getVersion());
            throw e;
        }
    }

    private SagaInstance requireInstance(String instanceId) {
        return repository.find(instanceId)
                .orElseThrow(() -> new SagaNotFoundException(instanceId));
    }

    private SagaDefinition requireDefinition(SagaInstance instance) {
        return registry.get(instance.getDefinitionName())
                .orElseThrow(() -> new IllegalStateException(
                        "saga definition not registered: " + instance.getDefinitionName()
                                + " (instanceId=" + instance.getInstanceId() + ")"));
    }

    private static boolean isTerminal(InstanceStatus status) {
        return status == InstanceStatus.COMPLETED || status == InstanceStatus.COMPENSATED;
    }

    // ---------------- 尝试记录辅助 ----------------

    /** 按 kind + 序号查找已有尝试记录；不存在返回 null。 */
    private static StepAttempt findAttempt(List<StepAttempt> attempts, String kind, int attemptNo) {
        for (StepAttempt a : attempts) {
            if (a.kind().equals(kind) && a.attemptNo() == attemptNo) {
                return a;
            }
        }
        return null;
    }

    /**
     * 历史上已取得确定结论（非在途）的最大尝试序号；在途标记不计入。
     * 用于新一轮驱动时为新尝试分配连续序号，同时保留全部历史轨迹。
     */
    private static int maxFinishedAttemptNo(List<StepAttempt> attempts, String kind) {
        int max = 0;
        for (StepAttempt a : attempts) {
            if (a.kind().equals(kind) && !a.inDoubt()) {
                max = Math.max(max, a.attemptNo());
            }
        }
        return max;
    }

    /** 用确定结论替换在途标记，保留原 executionId 与开始时刻。 */
    private static void finishAttempt(List<StepAttempt> attempts, String kind, int attemptNo,
                                      AttemptResult result) {
        int idx = -1;
        for (int k = attempts.size() - 1; k >= 0; k--) {
            if (attempts.get(k).attemptNo() == attemptNo && kind.equals(attempts.get(k).kind())) {
                idx = k;
                break;
            }
        }
        StepAttempt previous = attempts.get(idx);
        attempts.set(idx, new StepAttempt(kind, attemptNo, previous.executionId(),
                result.outcome(), result.error(), previous.startedAt(), Instant.now()));
    }

    private static String newExecutionId(SagaInstance instance, StepState step, String kind, int attemptNo) {
        return instance.getInstanceId() + ":" + step.getName() + ":" + kind + ":" + attemptNo + ":"
                + UUID.randomUUID();
    }

    private void sleepBackoff(Duration backoff, String instanceId, String stepName) {
        if (backoff.isZero()) {
            return;
        }
        log.info("[BACKOFF] instanceId={} step={} sleep={}", instanceId, stepName, backoff);
        try {
            Thread.sleep(backoff.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static SagaInstance newInstance(SagaDefinition definition, String instanceId, SagaContext initial) {
        SagaInstance instance = new SagaInstance();
        instance.setInstanceId(instanceId);
        instance.setDefinitionName(definition.name());
        instance.setStatus(InstanceStatus.RUNNING);
        instance.setContext(initial != null ? initial : new SagaContext());
        instance.setCreatedAt(Instant.now());
        instance.setUpdatedAt(instance.getCreatedAt());
        for (int i = 0; i < definition.steps().size(); i++) {
            instance.getSteps().add(new StepState(i, definition.steps().get(i).name()));
        }
        instance.addTrace("INSTANCE_SUBMITTED", "实例创建，定义=" + definition.name()
                + "，步骤数=" + definition.steps().size() + "，状态=RUNNING");
        return instance;
    }
}
