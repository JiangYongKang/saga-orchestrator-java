package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.ArrayList;
import java.util.List;

/** 单个步骤在某个实例内的运行态。 */
public class StepExecution {

    private String name;
    private int index;
    private StepStatus status = StepStatus.PENDING;
    private int attempts;
    private int compensationAttempts;
    private String lastError;
    private Object result;
    private long updatedMillis;
    private List<AttemptRecord> records = new ArrayList<>();

    public StepExecution() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public void setCompensationAttempts(int compensationAttempts) {
        this.compensationAttempts = compensationAttempts;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public long getUpdatedMillis() {
        return updatedMillis;
    }

    public void setUpdatedMillis(long updatedMillis) {
        this.updatedMillis = updatedMillis;
    }

    public List<AttemptRecord> getRecords() {
        return records;
    }

    public void setRecords(List<AttemptRecord> records) {
        this.records = records;
    }
}
