package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.time.Duration;

/**
 * 确定的超时策略：单次尝试在 timeout 内没有返回即判定为 TIMEOUT（不确定）。
 */
public record TimeoutPolicy(Duration timeout) {

    public TimeoutPolicy {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
    }
}
