package com.github.highcumontoa.sagaorchestratorjava.saga.action;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepPolicies;

/**
 * 一个编排步骤：名称 + 正向动作 + 补偿动作（补偿可空，表示该步骤无需补偿）+ 执行策略。
 */
public record SagaStep(String name, SagaAction action, Compensation compensation, StepPolicies policies) {

    public SagaStep(String name, SagaAction action, Compensation compensation) {
        this(name, action, compensation, StepPolicies.defaults());
    }
}
