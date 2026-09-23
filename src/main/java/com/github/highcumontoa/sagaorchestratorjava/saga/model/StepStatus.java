package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 单个步骤的状态机。 */
public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    COMPENSATING,
    COMPENSATION_FAILED,
    COMPENSATED,
    SKIPPED
}
