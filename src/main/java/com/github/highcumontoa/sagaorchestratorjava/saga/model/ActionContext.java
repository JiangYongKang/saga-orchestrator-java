package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.Map;

/**
 * 步骤执行/补偿时的上下文输入。
 *
 * @param dedupKey 本次逻辑动作的稳定去重标识：由 {@code 实例 + 步骤 + 阶段(FORWARD/COMPENSATE)}
 *                 确定性推导，跨重试、跨重启、跨重复触发保持不变；同一实例内不同步骤、
 *                 同一步骤的正向与补偿互不撞车。业务方应以其为幂等键：
 *                 副作用已落地但确认丢失（崩溃/超时）时，用它能认出重放并挡掉重复生效。
 */
public record ActionContext(
        String instanceId,
        String stepName,
        int attempt,
        String businessId,
        Map<String, Object> input,
        Map<String, Object> stepResults,
        String dedupKey) {
}
