package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/** 一次动作尝试的确定结论。 */
public enum AttemptOutcome {
    /** 尝试成功。 */
    SUCCESS,
    /** 尝试确定失败，可重试。 */
    FAILED,
    /** 尝试在超时窗口内无确定结论，视为不确定，可重试（重试由动作自身幂等保证不重复生效）。 */
    TIMEOUT
}
