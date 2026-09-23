package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 提交结果。deduplicated=true 表示本次为重复提交，直接返回已存在终态/实例。
 */
public record SubmitResult(SagaSnapshot snapshot, boolean deduplicated) {
}
