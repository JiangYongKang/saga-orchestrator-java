package com.github.highcumontoa.sagaorchestratorjava.tests.support;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.ActionContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaAction;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用正向动作：
 * <ul>
 *   <li>记录每次调用的 executionId，按 executionId 去重模拟业务侧幂等；</li>
 *   <li>effectiveCounts 统计“真实生效”次数，重试/超时复查不重复生效时恒为 1；</li>
 *   <li>可设置为前 failTimes 次调用抛异常，或 sleep 超过超时阈值模拟超时。</li>
 * </ul>
 */
public class RecordingAction implements SagaAction {

    public final List<String> executionIds = new CopyOnWriteArrayList<>();
    public final AtomicInteger invocationCount = new AtomicInteger();
    public final AtomicInteger effectiveCount = new AtomicInteger();
    public final ConcurrentHashMap.KeySetView<String, Boolean> seenExecutionIds =
            ConcurrentHashMap.newKeySet();

    private final String name;
    private int failTimes;
    private final long sleepMillis;

    public RecordingAction(String name) {
        this(name, 0, 0L);
    }

    public RecordingAction(String name, int failTimes, long sleepMillis) {
        this.name = name;
        this.failTimes = failTimes;
        this.sleepMillis = sleepMillis;
    }

    /** 返回一个每次调用都失败的动作（独立实例，便于链式构造）。 */
    public static RecordingAction failAlways(String name) {
        return new RecordingAction(name, Integer.MAX_VALUE, 0L);
    }

    /** 设置前 n 次调用失败（可用于恢复后重试的场景）。 */
    public RecordingAction failFirst(int times) {
        this.failTimes = times;
        return this;
    }

    /** 令该动作之后的每次调用都失败。 */
    public RecordingAction failAlways() {
        this.failTimes = Integer.MAX_VALUE;
        return this;
    }

    @Override
    public void execute(ActionContext ctx) throws Exception {
        invocationCount.incrementAndGet();
        executionIds.add(ctx.getExecutionId());
        // 模拟业务侧以 executionId 为键的幂等去重
        boolean firstTime = seenExecutionIds.add(ctx.getExecutionId());
        System.out.printf("    [动作] %s 调用 executionId=%s attempt=%d replay=%s 首次生效=%s%n",
                name, ctx.getExecutionId(), ctx.getAttemptNo(), ctx.isReplayOfInDoubtAttempt(), firstTime);
        if (sleepMillis > 0) {
            Thread.sleep(sleepMillis);
        }
        if (failTimes > 0) {
            failTimes--;
            throw new IllegalStateException(name + " simulated business failure");
        }
        if (firstTime) {
            effectiveCount.incrementAndGet();
        }
    }
}
