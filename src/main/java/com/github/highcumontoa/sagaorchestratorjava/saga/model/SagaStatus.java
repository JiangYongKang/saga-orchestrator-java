package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 编排实例状态机：实例级状态。 */
public enum SagaStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    COMPENSATING,
    COMPENSATION_FAILED,
    COMPENSATED,
    FAILED
}
