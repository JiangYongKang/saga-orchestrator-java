package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.time.Instant;

/**
 * 一次动作执行尝试的记录（正向或补偿共用）。
 *
 * <p>生命周期：先以 outcome=null 写入（在途标记，持久化后才调用动作），
 * 取得确定结论后原地替换为带结论的记录。崩溃恢复时发现 outcome=null 即
 * 表示上次尝试结论未知，必须用同一 executionId 复查（业务侧据此幂等去重）。
 *
 * @param kind        FORWARD / COMPENSATE
 * @param attemptNo   尝试序号，从 1 开始
 * @param executionId 本次尝试的执行 ID；在途复查复用同一值，新的重试生成新值
 * @param outcome     确定结论；null 表示在途/结论未知
 * @param error       失败/超时的原因描述（成功时为 null）
 * @param startedAt   开始时刻
 * @param finishedAt  结束时刻（在途时为 null）
 */
public record StepAttempt(
        String kind,
        int attemptNo,
        String executionId,
        AttemptOutcome outcome,
        String error,
        Instant startedAt,
        Instant finishedAt
) {
    public static final String FORWARD = "FORWARD";
    public static final String COMPENSATE = "COMPENSATE";

    public boolean inDoubt() {
        return outcome == null;
    }
}
