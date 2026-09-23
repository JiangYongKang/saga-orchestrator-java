package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 单个业务步骤的状态。 */
public enum StepStatus {
    /** 尚未执行。 */
    PENDING,
    /** 正向执行中（已下发尝试，等待确定结论）。 */
    RUNNING,
    /** 正向执行成功（补偿动作已登记）。 */
    SUCCEEDED,
    /** 正向执行失败（重试耗尽或超时）。 */
    FAILED,
    /** 待补偿：正向成功过，补偿尚未开始。 */
    COMPENSATION_PENDING,
    /** 补偿中：补偿尝试已下发，等待确定结论。 */
    COMPENSATING,
    /** 补偿失败：可幂等重试。 */
    COMPENSATION_FAILED,
    /** 补偿成功，终态（补偿动作不可再重复执行）。 */
    COMPENSATED
}
