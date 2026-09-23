package com.github.highcumontoa.sagaorchestratorjava.tests.support;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.ActionContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.Compensation;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 测试用补偿动作：语义同 {@link RecordingAction}，验证重复补偿触发的幂等性。 */
public class RecordingCompensation implements Compensation {

    public final List<String> executionIds = new CopyOnWriteArrayList<>();
    public final AtomicInteger invocationCount = new AtomicInteger();
    public final AtomicInteger effectiveCount = new AtomicInteger();
    public final ConcurrentHashMap.KeySetView<String, Boolean> seenExecutionIds =
            ConcurrentHashMap.newKeySet();

    /** 首次成功补偿完成的相对时刻（nanoTime），用于验证逆序。 */
    public volatile long firstFinishedAtNanos = Long.MAX_VALUE;

    private final String name;
    private int failTimes;

    public RecordingCompensation(String name) {
        this(name, 0);
    }

    public RecordingCompensation(String name, int failTimes) {
        this.name = name;
        this.failTimes = failTimes;
    }

    /** 模拟故障恢复：之后的调用均成功。 */
    public void fix() {
        this.failTimes = 0;
    }

    @Override
    public void compensate(ActionContext ctx) throws Exception {
        invocationCount.incrementAndGet();
        executionIds.add(ctx.getExecutionId());
        boolean firstTime = seenExecutionIds.add(ctx.getExecutionId());
        System.out.printf("    [补偿] %s 调用 executionId=%s attempt=%d replay=%s 首次生效=%s%n",
                name, ctx.getExecutionId(), ctx.getAttemptNo(), ctx.isReplayOfInDoubtAttempt(), firstTime);
        if (failTimes > 0) {
            failTimes--;
            throw new IllegalStateException(name + " simulated compensation failure");
        }
        if (firstTime) {
            effectiveCount.incrementAndGet();
            firstFinishedAtNanos = Math.min(firstFinishedAtNanos, System.nanoTime());
        }
    }
}
