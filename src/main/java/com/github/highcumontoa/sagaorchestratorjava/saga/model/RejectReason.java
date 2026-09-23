package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 请求被拒绝的可区分原因。 */
public enum RejectReason {
    /** 实例 ID 未知。 */
    UNKNOWN_INSTANCE,
    /** 当前状态不允许该操作（非法状态跳转）。 */
    ILLEGAL_TRANSITION,
    /** 并发冲突：存储版本不匹配（CAS 失败）。 */
    CONCURRENT_CONFLICT
}
