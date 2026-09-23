package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 单个步骤的正向/补偿重试与超时策略（持久化在定义中，编译期确定）。 */
public record StepPolicies(RetryPolicy forwardRetry, TimeoutPolicy forwardTimeout,
                          RetryPolicy compensationRetry, TimeoutPolicy compensationTimeout) {

    public static StepPolicies defaults() {
        return new StepPolicies(
                RetryPolicy.noRetry(),
                new TimeoutPolicy(java.time.Duration.ofSeconds(5)),
                RetryPolicy.noRetry(),
                new TimeoutPolicy(java.time.Duration.ofSeconds(5)));
    }
}
