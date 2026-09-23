package com.github.highcumontoa.sagaorchestratorjava.tests.support;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.Compensation;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaAction;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaStep;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.RetryPolicy;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepPolicies;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TimeoutPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** 构造测试用编排定义的夹具。 */
public final class SagaFixtures {

    private SagaFixtures() {
    }

    public static StepPolicies policies(int forwardMaxAttempts, Duration backoff, Duration forwardTimeout,
                                        int compensationMaxAttempts, Duration compensationTimeout) {
        return new StepPolicies(
                new RetryPolicy(forwardMaxAttempts, backoff),
                new TimeoutPolicy(forwardTimeout),
                new RetryPolicy(compensationMaxAttempts, backoff),
                new TimeoutPolicy(compensationTimeout));
    }

    public static StepPolicies fast(int forwardMaxAttempts, int compensationMaxAttempts) {
        return policies(forwardMaxAttempts, Duration.ofMillis(10), Duration.ofSeconds(2),
                compensationMaxAttempts, Duration.ofSeconds(2));
    }

    public static StepPolicies timeout(Duration timeout) {
        return policies(1, Duration.ZERO, timeout, 1, timeout);
    }

    public static StepPolicies timeout(int forwardMaxAttempts, Duration timeout) {
        return policies(forwardMaxAttempts, Duration.ZERO, timeout, 1, timeout);
    }

    public static Builder definition(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<SagaStep> steps = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder step(String stepName, SagaAction action, Compensation compensation, StepPolicies p) {
            steps.add(new SagaStep(stepName, action, compensation, p));
            return this;
        }

        public Builder step(String stepName, SagaAction action, Compensation compensation) {
            return step(stepName, action, compensation, fast(1, 1));
        }

        public SagaDefinition build() {
            return new SagaDefinition(name, List.copyOf(steps));
        }
    }
}
