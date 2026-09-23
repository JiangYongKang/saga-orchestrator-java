package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一个业务实例的完整可持久化状态。 */
public class SagaInstance {

    private String instanceId;
    private String type;
    private String businessId;
    private SagaStatus status = SagaStatus.PENDING;
    private long version;
    private Map<String, Object> input = new LinkedHashMap<>();
    private List<StepExecution> steps = new ArrayList<>();
    private List<TraceEvent> traces = new ArrayList<>();
    private long createdMillis;
    private long updatedMillis;

    public SagaInstance() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public void setStatus(SagaStatus status) {
        this.status = status;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public List<StepExecution> getSteps() {
        return steps;
    }

    public void setSteps(List<StepExecution> steps) {
        this.steps = steps;
    }

    public List<TraceEvent> getTraces() {
        return traces;
    }

    public void setTraces(List<TraceEvent> traces) {
        this.traces = traces;
    }

    public long getCreatedMillis() {
        return createdMillis;
    }

    public void setCreatedMillis(long createdMillis) {
        this.createdMillis = createdMillis;
    }

    public long getUpdatedMillis() {
        return updatedMillis;
    }

    public void setUpdatedMillis(long updatedMillis) {
        this.updatedMillis = updatedMillis;
    }
}
