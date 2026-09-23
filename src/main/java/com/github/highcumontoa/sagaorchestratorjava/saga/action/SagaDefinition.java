package com.github.highcumontoa.sagaorchestratorjava.saga.action;

import java.util.List;

/** 一个业务事务的步骤编排定义，步骤按列表顺序执行。 */
public record SagaDefinition(String name, List<SagaStep> steps) {
}
