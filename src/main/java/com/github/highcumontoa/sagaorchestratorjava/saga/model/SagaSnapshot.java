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
            List<AttemptRecord> records) {
    }
}
