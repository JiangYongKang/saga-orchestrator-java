package com.github.highcumontoa.sagaorchestratorjava.saga.demo;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaStep;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.RetryPolicy;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepPolicies;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TimeoutPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;

/**
 * 演示编排定义：下单 -> 扣库存 -> 扣余额 -> 发货。
 * 每个动作以 executionId 为幂等键（演示中仅打印日志），并登记逆序补偿。
 */
@Configuration
public class DemoSagaDefinition {

    private static final Logger log = LoggerFactory.getLogger(DemoSagaDefinition.class);
    public static final String NAME = "demo-order";

    @Bean
    public SagaDefinition demoOrderDefinition(SagaDefinitionRegistry registry) {
        StepPolicies policies = new StepPolicies(
                new RetryPolicy(2, Duration.ofMillis(50)),
                new TimeoutPolicy(Duration.ofSeconds(3)),
                new RetryPolicy(2, Duration.ofMillis(50)),
                new TimeoutPolicy(Duration.ofSeconds(3)));

        List<SagaStep> steps = List.of(
                new SagaStep("create-order",
                        ctx -> log.info("[动作] create-order executionId={}", ctx.getExecutionId()),
                        ctx -> log.info("[补偿] cancel-order executionId={}", ctx.getExecutionId()),
                        policies),
                new SagaStep("reserve-stock",
                        ctx -> log.info("[动作] reserve-stock executionId={}", ctx.getExecutionId()),
                        ctx -> log.info("[补偿] release-stock executionId={}", ctx.getExecutionId()),
                        policies),
                new SagaStep("deduct-balance",
                        ctx -> log.info("[动作] deduct-balance executionId={}", ctx.getExecutionId()),
                        ctx -> log.info("[补偿] refund-balance executionId={}", ctx.getExecutionId()),
                        policies),
                new SagaStep("ship",
                        ctx -> log.info("[动作] ship executionId={}", ctx.getExecutionId()),
                        ctx -> log.info("[补偿] recall-shipment executionId={}", ctx.getExecutionId()),
                        policies));

        SagaDefinition definition = new SagaDefinition(NAME, steps);
        registry.register(definition);
        return definition;
    }
}
