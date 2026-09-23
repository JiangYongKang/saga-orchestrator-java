package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;

/** 非法状态跳转：当前状态不允许执行请求的操作。 */
public class IllegalSagaTransitionException extends SagaException {
    public IllegalSagaTransitionException(String message) {
        super(RejectReason.ILLEGAL_TRANSITION, message);
    }
}
