package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 场景二：正向执行中途失败，已成功步骤按逆序补偿。 */
class CompensationFlowTest {

    private static final Logger log = LoggerFactory.getLogger(CompensationFlowTest.class);

    @Test
    void failureTriggersReverseCompensation() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        RecordingAction a1 = new RecordingAction("step1");
        RecordingAction a2 = new RecordingAction("step2");
        RecordingAction a3 = new RecordingAction("step3");
        RecordingAction a4 = new RecordingAction("step4");
        RecordingCompensation c1 = new RecordingCompensation("comp1");
        RecordingCompensation c2 = new RecordingCompensation("comp2");
        RecordingCompensation c3 = new RecordingCompensation("comp3");
        // step3 成功后 step4 失败；期望补偿顺序 comp3 -> comp2 -> comp1
        SagaDefinition def = SagaFixtures.definition("order-flow")
                .step("step1", a1, c1)
                .step("step2", a2, c2)
                .step("step3", a3, c3)
                .step("step4", a4.failAlways(), null)
                .build();

        String id = "TX-COMP-1";
        log.info("输入: submit instanceId={} step4 必然失败，期望逆序补偿", id);
        SagaInstance result = orchestrator.submit(def, id, new SagaContext());

        log.info("判定: 实例状态={} 期望=COMPENSATED", result.getStatus());
        assertEquals(InstanceStatus.COMPENSATED, result.getStatus());
        assertEquals(StepStatus.COMPENSATED, result.getSteps().get(0).getStatus());
        assertEquals(StepStatus.COMPENSATED, result.getSteps().get(1).getStatus());
        assertEquals(StepStatus.COMPENSATED, result.getSteps().get(2).getStatus());
        assertEquals(StepStatus.FAILED, result.getSteps().get(3).getStatus());

        // 逆序验证：comp3 先于 comp2 先于 comp1
        long t3 = c3.firstFinishedAtNanos;
        long t2 = c2.firstFinishedAtNanos;
        long t1 = c1.firstFinishedAtNanos;
        log.info("判定: 补偿完成时刻 comp3={} < comp2={} < comp1={}", t3, t2, t1);
        org.junit.jupiter.api.Assertions.assertTrue(t3 < t2 && t2 < t1, "补偿必须严格逆序");

        // 失败步骤本身不补偿
        assertEquals(1, a1.effectiveCount.get());
        assertEquals(1, a2.effectiveCount.get());
        assertEquals(1, a3.effectiveCount.get());
        assertEquals(0, a4.effectiveCount.get());
        assertEquals(1, c1.effectiveCount.get());
        assertEquals(1, c2.effectiveCount.get());
        assertEquals(1, c3.effectiveCount.get());

        // 轨迹必须能解释全过程
        List<String> types = result.getTrace().stream().map(t -> t.eventType()).toList();
        log.info("轨迹事件序列={}", types);
        org.junit.jupiter.api.Assertions.assertTrue(types.contains("STATE_TRANSITION"));
        log.info("最终轨迹:\n{}", String.join("\n",
                result.getTrace().stream().map(t -> "  " + t.seq() + " " + t.detail()).toList()));
    }
}
