package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.Map;

/**
 * 步骤执行/补偿时的上下文输入。
 *
 * @param actionKey 本次逻辑动作（正向或补偿）的稳定去重标识，跨重试、跨重启、
 *                  跨重复触发保持不变；业务方用它判重，挡掉「副作用已落地但
 *                  确认丢失」时的重复生效。派生规则见 {@link ActionKeys}。
 */
public record ActionContext(
        String instanceId,
        String stepName,
        int attempt,
        String businessId,
        Map<String, Object> input,
        Map<String, Object> stepResults,
        String actionKey) {
}
