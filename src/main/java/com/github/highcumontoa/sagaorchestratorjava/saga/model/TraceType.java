package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 执行轨迹事件类型，用于可解释查询。 */
public enum TraceType {
    SUBMITTED,
    SUBMIT_DEDUP,
    FORWARD_START,
    STEP_START,
    STEP_RETRY,
    STEP_TIMEOUT,
    STEP_SUCCEEDED,
    STEP_SKIPPED,
    STEP_FAILED,
    COMPENSATION_START,
    STEP_COMPENSATE_START,
    STEP_COMPENSATE_RETRY,
    STEP_COMPENSATED,
    STEP_COMPENSATE_FAILED,
    COMPENSATION_FAILED,
    COMPLETED,
    COMPENSATED_ALL,
    RECOVERED,
    /** 恢复时发现「动作已下发、结果未确认」的在途守卫记录，决定以同一去重标识重放。 */
    IN_FLIGHT_RECOVERED,
    REJECTED
}
