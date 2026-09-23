package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 业务补偿动作。必须幂等：补偿失败后重复触发补偿时会被再次调用。
 */
@FunctionalInterface
public interface SagaCompensation {

    void compensate(ActionContext context) throws Exception;
}
