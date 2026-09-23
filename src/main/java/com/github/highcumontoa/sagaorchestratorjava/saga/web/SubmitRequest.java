package com.github.highcumontoa.sagaorchestratorjava.saga.web;

/** 提交实例请求：实例 ID（业务幂等键）+ 已注册的定义名。 */
public record SubmitRequest(String instanceId, String definitionName) {
}
