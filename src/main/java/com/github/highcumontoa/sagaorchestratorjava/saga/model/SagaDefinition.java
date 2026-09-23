package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.List;

/**
 * 一个 Saga 编排模板：有序步骤列表，每步可登记补偿动作。
 */
public record SagaDefinition(String type, List<StepDefinition> steps) {

    public SagaDefinition {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("saga type must not be blank");
        }
        steps = List.copyOf(steps);
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("saga must have at least one step");
        }
    }
}
