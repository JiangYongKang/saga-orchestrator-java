package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

/** 对非法操作的可区分拒绝原因。 */
public enum RejectReason {
    UNKNOWN_INSTANCE,
    ILLEGAL_STATE,
    CONCURRENT_CONFLICT,
    VERSION_CONFLICT,
    CONFLICTING_DEFINITION
}
