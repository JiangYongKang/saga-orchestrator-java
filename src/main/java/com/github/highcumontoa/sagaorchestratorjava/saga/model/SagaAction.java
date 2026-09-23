package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 业务正向动作。实现方应保证幂等：超时重试时可能被再次调用。
 */
@FunctionalInterface
public interface SagaAction {

    /** @return 可被后续步骤与补偿读取的步骤结果（可为 null）。 */
    Object execute(ActionContext context) throws Exception;
}
