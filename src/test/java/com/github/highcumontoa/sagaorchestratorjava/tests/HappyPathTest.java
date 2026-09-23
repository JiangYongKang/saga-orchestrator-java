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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** 场景一：三个步骤全部正向成功，实例收敛到 COMPLETED。 */
class HappyPathTest {

    private static final Logger log = LoggerFactory.getLogger(HappyPathTest.class);

    @Test
    void allStepsSucceed() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        RecordingAction a1 = new RecordingAction("step1");
        RecordingAction a2 = new RecordingAction("step2");
        RecordingAction a3 = new RecordingAction("step3");
        RecordingCompensation c1 = new RecordingCompensation("comp1");
        RecordingCompensation c2 = new RecordingCompensation("comp2");
        RecordingCompensation c3 = new RecordingCompensation("comp3");
        SagaDefinition def = SagaFixtures.definition("order-flow")
                .step("step1", a1, c1)
                .step("step2", a2, c2)
                .step("step3", a3, c3)
                .build();

        String id = "TX-HAPPY-1";
        log.info("输入: submit instanceId={} 步骤=step1,step2,step3 全部成功", id);
        SagaInstance result = orchestrator.submit(def, id, new SagaContext());

        log.info("判定: 实例状态={} 期望=COMPLETED", result.getStatus());
        assertEquals(InstanceStatus.COMPLETED, result.getStatus());
        assertEquals(StepStatus.SUCCEEDED, result.getSteps().get(0).getStatus());
        assertEquals(StepStatus.SUCCEEDED, result.getSteps().get(1).getStatus());
        assertEquals(StepStatus.SUCCEEDED, result.getSteps().get(2).getStatus());
        assertEquals(1, a1.effectiveCount.get());
        assertEquals(1, a2.effectiveCount.get());
        assertEquals(1, a3.effectiveCount.get());
        assertEquals(0, c1.invocationCount.get());
        assertEquals(0, c2.invocationCount.get());
        assertEquals(0, c3.invocationCount.get());
        assertNotNull(result.getCreatedAt());
        log.info("轨迹:\n{}", String.join("\n",
                result.getTrace().stream().map(t -> "  " + t.seq() + " " + t.eventType() + " " + t.detail()).toList()));
    }
}
