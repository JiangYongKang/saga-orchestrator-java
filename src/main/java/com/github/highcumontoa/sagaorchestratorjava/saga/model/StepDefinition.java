package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 一个业务步骤的定义。
 *
 * @param maxAttempts      正向最大尝试次数（含首次），&lt;1 视为 1
 * @param timeoutMillis    单次尝试超时，&lt;=0 表示不限制
 * @param retryBackoffMillis 两次尝试之间的固定退避
 */
public record StepDefinition(
        String name,
        SagaAction action,
        SagaCompensation compensation,
        int maxAttempts,
        long timeoutMillis,
        long retryBackoffMillis) {

    public StepDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("step name must not be blank");
        }
        if (action == null) {
            throw new IllegalArgumentException("step action must not be null");
        }
        if (maxAttempts < 1) {
            maxAttempts = 1;
        }
        if (timeoutMillis < 0) {
            timeoutMillis = 0;
        }
        if (retryBackoffMillis < 0) {
            retryBackoffMillis = 0;
        }
    }
}
