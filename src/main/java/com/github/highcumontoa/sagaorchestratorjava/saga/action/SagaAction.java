package com.github.highcumontoa.sagaorchestratorjava.saga.action;

/**
 * 业务正向动作。实现必须对同一 executionId 的重复调用幂等
 * （崩溃恢复 / 超时复查可能再次调用）。
 */
@FunctionalInterface
public interface SagaAction {
    void execute(ActionContext ctx) throws Exception;
}
