package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.AttemptOutcome;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepAttempt;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 场景五：确定的重试与超时策略；重试不造成步骤重复生效。 */
class RetryTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(RetryTimeoutTest.class);

    @Test
    void retryThenSucceedsExecutesExactlyOnceEffectively() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        RecordingAction flaky = new RecordingAction("flaky", 2, 0L); // 前两次失败，第三次成功
        SagaDefinition def = SagaFixtures.definition("retry-flow")
                .step("flaky", flaky, null,
                        SagaFixtures.policies(3, Duration.ofMillis(5), Duration.ofSeconds(2), 1, Duration.ofSeconds(2)))
                .build();

        log.info("输入: 步骤前 2 次失败，第 3 次成功，maxAttempts=3");
        SagaInstance result = orchestrator.submit(def, "TX-RETRY-1", new SagaContext());

        assertEquals(InstanceStatus.COMPLETED, result.getStatus());
        assertEquals(3, flaky.invocationCount.get(), "应当恰好调用 3 次");
        assertEquals(1, flaky.effectiveCount.get(), "实际生效必须只有一次");
        assertEquals(3, result.getSteps().get(0).getForwardAttempts().size());
        assertEquals(AttemptOutcome.FAILED, result.getSteps().get(0).getForwardAttempts().get(0).outcome());
        assertEquals(AttemptOutcome.FAILED, result.getSteps().get(0).getForwardAttempts().get(1).outcome());
        assertEquals(AttemptOutcome.SUCCESS, result.getSteps().get(0).getForwardAttempts().get(2).outcome());
        log.info("判定: 尝试结论序列=FAILED,FAILED,SUCCESS；调用=3，生效=1，执行 ID 各不相同={}",
                flaky.executionIds.stream().distinct().count() == 3);
        assertEquals(3, flaky.executionIds.stream().distinct().count(), "每次重试使用不同 executionId");
    }

    @Test
    void timeoutIsRecordedAsDeterministicOutcomeAndRetriedWithNewExecutionId() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        // 每次调用都睡 400ms，超过 80ms 超时；maxAttempts=2，两次都超时 -> 失败并补偿
        RecordingAction slow = new RecordingAction("slow", 0, 400L);
        SagaDefinition def = SagaFixtures.definition("timeout-flow")
                .step("s1", new RecordingAction("s1"), ctx -> { })
                .step("slow", slow, null, SagaFixtures.timeout(2, Duration.ofMillis(80)))
                .build();

        long t0 = System.nanoTime();
        log.info("输入: slow 每次耗时400ms，超时阈值80ms，maxAttempts=2");
        SagaInstance result = orchestrator.submit(def, "TX-TIMEOUT-1", new SagaContext());
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        assertEquals(InstanceStatus.COMPENSATED, result.getStatus());
        var attempts = result.getSteps().get(1).getForwardAttempts();
        assertEquals(2, attempts.size());
        assertEquals(AttemptOutcome.TIMEOUT, attempts.get(0).outcome(), "第一次尝试必须判定为 TIMEOUT");
        assertEquals(AttemptOutcome.TIMEOUT, attempts.get(1).outcome());
        assertEquals(2, slow.executionIds.stream().distinct().count(), "超时后的重试使用新的 executionId");
        log.info("判定: 两次尝试均为 TIMEOUT，新执行ID重试；判定耗时={}ms（应约为两次超时之和）", elapsedMs);
        // 判定依据必须可在轨迹中解释
        long timeoutTraces = result.getTrace().stream()
                .filter(e -> e.eventType().equals("ATTEMPT_RESULT") && e.detail().contains("TIMEOUT"))
                .count();
        assertEquals(2, timeoutTraces, "轨迹需记录两次超时判定依据");
    }
}
