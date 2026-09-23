package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.time.Instant;

/**
 * 执行轨迹中的一条事件，用于对外解释实例的状态由来。
 *
 * @param seq        单调递增序号
 * @param time       事件时刻
 * @param eventType  事件类型（INSTANCE_SUBMITTED / STATE_TRANSITION / ATTEMPT_RESULT / REJECTED / RECOVERED 等）
 * @param detail     人类可读的判定依据
 */
public record TraceEvent(
        long seq,
        Instant time,
        String eventType,
        String detail
) {
}
