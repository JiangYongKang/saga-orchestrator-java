package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;

/** 未知实例：实例 ID 在存储中不存在。 */
public class SagaNotFoundException extends SagaException {
    public SagaNotFoundException(String instanceId) {
        super(RejectReason.UNKNOWN_INSTANCE, "saga instance not found: " + instanceId);
    }
}
