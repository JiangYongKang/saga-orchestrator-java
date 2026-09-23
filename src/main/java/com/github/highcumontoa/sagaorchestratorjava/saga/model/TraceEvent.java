package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 一条执行轨迹事件。 */
public class TraceEvent {

    private long millis;
    private TraceType type;
    private String stepName;
    private String detail;

    public TraceEvent() {
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public TraceType getType() {
        return type;
    }

    public void setType(TraceType type) {
        this.type = type;
    }

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
}
