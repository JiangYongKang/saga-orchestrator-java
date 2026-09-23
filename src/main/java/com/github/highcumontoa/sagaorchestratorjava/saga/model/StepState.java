package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.ArrayList;
import java.util.List;

/** 单个步骤在实例内的可变执行状态（持久化）。 */
public class StepState {

    private int index;
    private String name;
    private StepStatus status = StepStatus.PENDING;
    private List<StepAttempt> forwardAttempts = new ArrayList<>();
    private List<StepAttempt> compensationAttempts = new ArrayList<>();

    public StepState() {
    }

    public StepState(int index, String name) {
        this.index = index;
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public StepStatus getStatus() {
        return status;
    }

    public void setStatus(StepStatus status) {
        this.status = status;
    }

    public List<StepAttempt> getForwardAttempts() {
        return forwardAttempts;
    }

    public void setForwardAttempts(List<StepAttempt> forwardAttempts) {
        this.forwardAttempts = forwardAttempts;
    }

    public List<StepAttempt> getCompensationAttempts() {
        return compensationAttempts;
    }

    public void setCompensationAttempts(List<StepAttempt> compensationAttempts) {
        this.compensationAttempts = compensationAttempts;
    }

    /** 最新一次正向尝试的序号，尚无尝试返回 0。 */
    public int forwardAttemptCount() {
        return forwardAttempts.size();
    }

    /** 最新一次补偿尝试的序号，尚无尝试返回 0。 */
    public int compensationAttemptCount() {
        return compensationAttempts.size();
    }
}
