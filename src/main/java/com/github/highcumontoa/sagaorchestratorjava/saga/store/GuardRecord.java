package com.github.highcumontoa.sagaorchestratorjava.saga.store;

/**
 * 幂等守卫记录：标记某个“实例+步骤+阶段”的业务动作是否已经生效。
 * IN_FLIGHT 表示已开始但结果未知（崩溃后视为失效，允许重放）。
 */
public class GuardRecord {

    private String key;
    private String state; // IN_FLIGHT / DONE / FAILED
    private int attempt; // 本次 IN_FLIGHT/DONE 对应的尝试序号（fencing token）
    private String resultJson;
    private String error;
    private long updatedMillis;

    public GuardRecord() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public String getResultJson() {
        return resultJson;
    }

    public void setResultJson(String resultJson) {
        this.resultJson = resultJson;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public long getUpdatedMillis() {
        return updatedMillis;
    }

    public void setUpdatedMillis(long updatedMillis) {
        this.updatedMillis = updatedMillis;
    }
}
