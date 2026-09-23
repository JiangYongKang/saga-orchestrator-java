package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 单次尝试的执行记录（正向或补偿），用于轨迹与判定依据解释。
 */
public class AttemptRecord {

    private int attempt;
    private String phase; // FORWARD / COMPENSATE
    private String status; // SUCCESS / FAILURE / TIMEOUT
    private String error;
    private long startMillis;
    private long endMillis;

    public AttemptRecord() {
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public String getPhase() {
        return phase;
    }

    public void setPhase(String phase) {
        this.phase = phase;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public long getStartMillis() {
        return startMillis;
    }

    public void setStartMillis(long startMillis) {
        this.startMillis = startMillis;
    }

    public long getEndMillis() {
        return endMillis;
    }

    public void setEndMillis(long endMillis) {
        this.endMillis = endMillis;
    }
}
