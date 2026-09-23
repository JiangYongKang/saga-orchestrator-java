package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.AttemptOutcome;

/** 一次尝试的执行结论。 */
public record AttemptResult(AttemptOutcome outcome, String error) {

    public static AttemptResult success() {
        return new AttemptResult(AttemptOutcome.SUCCESS, null);
    }

    public static AttemptResult failed(String error) {
        return new AttemptResult(AttemptOutcome.FAILED, error);
    }

    public static AttemptResult timeout(String error) {
        return new AttemptResult(AttemptOutcome.TIMEOUT, error);
    }
}
