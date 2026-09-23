package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.time.Duration;

/**
 * 确定的重试策略：最大尝试次数与重试退避。
 * maxAttempts=1 表示不重试。重试不造成重复生效由动作对 executionId 的幂等保证。
 */
public record RetryPolicy(int maxAttempts, Duration backoff) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (backoff == null || backoff.isNegative()) {
            throw new IllegalArgumentException("backoff must not be null or negative");
        }
    }

    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, Duration.ZERO);
    }
}
