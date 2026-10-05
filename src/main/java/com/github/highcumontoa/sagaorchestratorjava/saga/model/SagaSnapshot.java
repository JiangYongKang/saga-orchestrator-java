package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.List;

/**
 * 对外查询视图：实例状态 + 每步状态 + 执行轨迹，保证可解释。
 */
public record SagaSnapshot(
        String instanceId,
        String type,
        String businessId,
        SagaStatus status,
        long version,
        List<StepView> steps,
        List<TraceEvent> traces,
        long updatedMillis) {

    /**
     * 单个步骤的解释视图。
     *
     * @param forwardDedupKey      正向动作的稳定去重标识（业务侧幂等键）
     * @param compensationDedupKey 补偿动作的稳定去重标识
     * @param inFlightPhase        当前处于「动作已下发、结果未确认」在途态的阶段
     *                             （FORWARD / COMPENSATE），不在在途态则为 null；
     *                             崩溃重启后、恢复重放前可通过它直接看出未知结果的悬空动作
     */
    public record StepView(
            String name,
            int index,
            StepStatus status,
            int attempts,
            int compensationAttempts,
            String lastError,
            Object result,
            List<AttemptRecord> records,
            String forwardDedupKey,
            String compensationDedupKey,
            String inFlightPhase) {
    }
}
