package com.github.highcumontoa.sagaorchestratorjava.saga.action;

/**
 * 补偿动作。实现必须幂等：同一 executionId 的重复调用（重复补偿触发）
 * 不得重复产生副作用。
 */
@FunctionalInterface
public interface Compensation {
    void compensate(ActionContext ctx) throws Exception;
}
