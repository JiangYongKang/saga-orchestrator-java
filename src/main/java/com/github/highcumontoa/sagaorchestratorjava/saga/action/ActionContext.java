package com.github.highcumontoa.sagaorchestratorjava.saga.action;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;

/**
 * 传递给正向/补偿动作的执行上下文。
 * executionId 为动作侧幂等键：同一逻辑动作的重试会携带不同 executionId，
 * 但崩溃恢复后对“不确定尝试”的复查会复用相同 executionId，业务侧据此去重。
 */
public class ActionContext {

    private final String instanceId;
    private final String stepName;
    private final String executionId;
    private final int attemptNo;
    private final SagaContext sagaContext;
    private final boolean replayOfInDoubtAttempt;

    public ActionContext(String instanceId, String stepName, String executionId, int attemptNo,
                         SagaContext sagaContext, boolean replayOfInDoubtAttempt) {
        this.instanceId = instanceId;
        this.stepName = stepName;
        this.executionId = executionId;
        this.attemptNo = attemptNo;
        this.sagaContext = sagaContext;
        this.replayOfInDoubtAttempt = replayOfInDoubtAttempt;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getStepName() {
        return stepName;
    }

    public String getExecutionId() {
        return executionId;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public SagaContext getSagaContext() {
        return sagaContext;
    }

    public boolean isReplayOfInDoubtAttempt() {
        return replayOfInDoubtAttempt;
    }
}
