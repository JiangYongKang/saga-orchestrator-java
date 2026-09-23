package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

/**
 * 所有被明确拒绝的操作统一抛出，携带 {@link RejectReason} 供调用方区分。
 */
public class SagaRejectedException extends RuntimeException {

    private final RejectReason reason;

    public SagaRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
