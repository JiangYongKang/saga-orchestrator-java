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

    /** 单个步骤的解释视图。 */
    public record StepView(
            String name,
            int index,
            StepStatus status,
            int attempts,
            int compensationAttempts,
            String lastError,
            Object result,
            List<AttemptRecord> records,
            /** 正向动作的稳定去重标识（派生规则见 ActionKeys）。 */
            String actionKey,
            /** 补偿动作的稳定去重标识。 */
            String compensationKey,
            /** true 表示「动作已下发、结果尚未确认」的在途中间态（含崩溃遗留）。 */
            boolean inFlight,
            /** 在途动作对应的尝试序号（fencing token）；非在途时为 0。 */
            int inFlightAttempt) {
    }
}
