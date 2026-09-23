package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;

/** 所有可明确区分拒绝原因的编排异常基类。 */
public class SagaException extends RuntimeException {

    private final RejectReason reason;

    public SagaException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
