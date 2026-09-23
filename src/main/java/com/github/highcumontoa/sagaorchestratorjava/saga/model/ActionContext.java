package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.Map;

/**
 * 步骤执行/补偿时的上下文输入。
 */
public record ActionContext(
        String instanceId,
        String stepName,
        int attempt,
        String businessId,
        Map<String, Object> input,
        Map<String, Object> stepResults) {
}
