package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** 一个业务事务实例的完整可持久化状态。 */
public class SagaInstance {

    private String instanceId;
    private String definitionName;
    private InstanceStatus status = InstanceStatus.RUNNING;
    private int currentStepIndex;
    private List<StepState> steps = new ArrayList<>();
    private SagaContext context = new SagaContext();
    private List<TraceEvent> trace = new ArrayList<>();
    private long version;
    private Instant createdAt;
    private Instant updatedAt;
    /** 补偿失败时首个失败步骤的原因摘要，便于解释。 */
    private String lastFailureReason;

    public SagaInstance() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getDefinitionName() {
        return definitionName;
    }

    public void setDefinitionName(String definitionName) {
        this.definitionName = definitionName;
    }

    public InstanceStatus getStatus() {
        return status;
    }

    public void setStatus(InstanceStatus status) {
        this.status = status;
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public void setCurrentStepIndex(int currentStepIndex) {
        this.currentStepIndex = currentStepIndex;
    }

    public List<StepState> getSteps() {
        return steps;
    }

    public void setSteps(List<StepState> steps) {
        this.steps = steps;
    }

    public SagaContext getContext() {
        return context;
    }

    public void setContext(SagaContext context) {
        this.context = context;
    }

    public List<TraceEvent> getTrace() {
        return trace;
    }

    public void setTrace(List<TraceEvent> trace) {
        this.trace = trace;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getLastFailureReason() {
        return lastFailureReason;
    }

    public void setLastFailureReason(String lastFailureReason) {
        this.lastFailureReason = lastFailureReason;
    }

    /** 追加一条轨迹事件，序号由实例内单调递增。 */
    public void addTrace(String eventType, String detail) {
        trace.add(new TraceEvent(trace.size() + 1L, Instant.now(), eventType, detail));
    }
}
