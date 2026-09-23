package com.github.highcumontoa.sagaorchestratorjava.saga.exception;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;

/** 并发冲突：乐观版本号不匹配，CAS 更新被拒绝。 */
public class SagaConcurrentConflictException extends SagaException {
    public SagaConcurrentConflictException(String instanceId, long expectedVersion, long actualVersion) {
        super(RejectReason.CONCURRENT_CONFLICT,
                "concurrent conflict on instance " + instanceId
                        + ": expected version " + expectedVersion + " but was " + actualVersion);
    }
}
