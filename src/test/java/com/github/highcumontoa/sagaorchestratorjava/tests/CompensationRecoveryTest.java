package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 场景九：补偿在途标记落盘后崩溃，恢复必须复用同一 executionId 复查，不得跳过/重复补偿。 */
class CompensationRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CompensationRecoveryTest.class);

    private static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash() {
            super("simulated crash during compensation dispatch");
        }
    }

    @Test
    void crashDuringCompensationDispatchResumesWithSameExecutionId() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();

        RecordingAction s1 = new RecordingAction("s1");
        RecordingAction s2 = new RecordingAction("s2");
        RecordingCompensation c1 = new RecordingCompensation("c1");
        RecordingCompensation c2 = new RecordingCompensation("c2");
        SagaDefinition def = SagaFixtures.definition("comp-recovery-flow")
                .step("s1", s1, c1)
                .step("s2", s2, c2)
                .step("s3", RecordingAction.failAlways("s3"), null)
                .build();

        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, registry);
        // 在 c2（第一个被补偿的动作）的在途标记落盘后崩溃
        orchestrator.setInFlightCrashHook((inst, step, kind, no) -> {
            if ("COMPENSATE".equals(kind) && "s2".equals(step.getName())) {
                log.info("    [模拟崩溃] 补偿 {} 在途标记落盘后进程终止", step.getName());
                throw new SimulatedCrash();
            }
        });

        String id = "TX-CRECOV-1";
        log.info("输入: submit id={}，s3 失败触发逆序补偿，c2 在途时崩溃", id);
        assertThrows(SimulatedCrash.class, () -> orchestrator.submit(def, id, new SagaContext()));

        var snapshot = repo.find(id).orElseThrow();
        assertEquals(InstanceStatus.COMPENSATING, snapshot.getStatus());
        assertEquals(StepStatus.COMPENSATING, snapshot.getSteps().get(1).getStatus());
        var marker = snapshot.getSteps().get(1).getCompensationAttempts().get(0);
        String crashExecutionId = marker.executionId();
        org.junit.jupiter.api.Assertions.assertTrue(marker.inDoubt());
        log.info("崩溃后状态: status=COMPENSATING, s2=COMPENSATING(在途), s1=COMPENSATION_PENDING");

        // 重新初始化并恢复
        SagaOrchestrator revived = new SagaOrchestrator(repo, registry);
        revived.recoverAll();

        var done = revived.get(id);
        assertEquals(InstanceStatus.COMPENSATED, done.getStatus());
        assertEquals(1, c2.invocationCount.get(), "c2 只被复查一次");
        assertEquals(1, c1.invocationCount.get(), "c1 在恢复后补偿一次");
        assertEquals(1, c2.effectiveCount.get());
        assertEquals(1, c1.effectiveCount.get());
        var recovered = done.getSteps().get(1).getCompensationAttempts().get(0);
        assertEquals(crashExecutionId, recovered.executionId(),
                "补偿复查必须复用崩溃前在途标记的同一 executionId");
        log.info("判定: 恢复复用补偿 executionId，c2 复查成功后继续 c1，实例 -> COMPENSATED");
    }
}
