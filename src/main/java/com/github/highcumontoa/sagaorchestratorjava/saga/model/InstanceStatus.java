package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 编排实例生命周期状态。 */
public enum InstanceStatus {
    /** 正向执行中。 */
    RUNNING,
    /** 正向全部成功，终态。 */
    COMPLETED,
    /** 失败后正在按逆序补偿。 */
    COMPENSATING,
    /** 补偿全部完成，终态。 */
    COMPENSATED,
    /** 补偿过程中存在失败步骤，可重试，非终态。 */
    COMPENSATION_FAILED
}
