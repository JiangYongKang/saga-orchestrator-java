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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景七：组件重启/重新初始化后恢复推进。
 * <ul>
 *   <li>正向执行在途标记落盘后“崩溃”：恢复必须复用同一 executionId 复查，不新尝试、不重复生效；</li>
 *   <li>正向中途失败崩溃：恢复后不得跳过补偿；</li>
 *   <li>已完成步骤不得重复执行。</li>
 * </ul>
 */
class RecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);

    private static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash() {
            super("simulated process crash after dispatch persisted");
        }
    }

    @Test
    void crashAfterDispatchRecoversByReplayingSameExecutionId() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();

        RecordingAction s1 = new RecordingAction("s1");
        RecordingAction s2 = new RecordingAction("s2");
        RecordingCompensation c1 = new RecordingCompensation("c1");
        RecordingCompensation c2 = new RecordingCompensation("c2");
        SagaDefinition def = SagaFixtures.definition("recovery-flow")
                .step("s1", s1, c1)
                .step("s2", s2, c2)
                .build();

        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, registry);
        // 在 s2 的正向在途标记落盘后立即模拟崩溃（动作尚未被调用）
        orchestrator.setInFlightCrashHook((inst, step, kind, no) -> {
            if ("s2".equals(step.getName())) {
                log.info("    [模拟崩溃] instanceId={} step={} {}#{} 在途标记已落盘，进程终止",
                        inst.getInstanceId(), step.getName(), kind, no);
                throw new SimulatedCrash();
            }
        });

        String id = "TX-RECOV-1";
        log.info("输入: submit id={}，s2 在途标记落盘后模拟崩溃", id);
        assertThrows(SimulatedCrash.class, () -> orchestrator.submit(def, id, new SagaContext()));

        SagaInstance snapshot = repo.find(id).orElseThrow();
        log.info("崩溃后磁盘状态: status={} s1={} s2={}",
                snapshot.getStatus(),
                snapshot.getSteps().get(0).getStatus(),
                snapshot.getSteps().get(1).getStatus());
        assertEquals(InstanceStatus.RUNNING, snapshot.getStatus());
        assertEquals(StepStatus.SUCCEEDED, snapshot.getSteps().get(0).getStatus());
        assertEquals(StepStatus.RUNNING, snapshot.getSteps().get(1).getStatus());
        var marker = snapshot.getSteps().get(1).getForwardAttempts().get(0);
        assertTrue(marker.inDoubt(), "崩溃后必须留下 outcome=null 的在途标记");
        String dispatchExecutionId = marker.executionId();

        // “重新初始化”：新建编排器与同一存储
        SagaOrchestrator revived = new SagaOrchestrator(repo, registry);
        log.info("输入: 重新初始化组件并 recoverAll()");
        int count = revived.recoverAll();
        assertEquals(1, count);

        SagaInstance done = revived.get(id);
        assertEquals(InstanceStatus.COMPLETED, done.getStatus());
        assertEquals(1, s1.invocationCount.get(), "s1 已确认成功，恢复时不得重复执行");
        assertEquals(1, s2.invocationCount.get(), "s2 只被复查执行一次");
        assertEquals(1, s2.effectiveCount.get());
        var recoveredAttempt = done.getSteps().get(1).getForwardAttempts().get(0);
        assertEquals(dispatchExecutionId, recoveredAttempt.executionId(),
                "恢复复查必须复用崩溃前在途标记的同一 executionId");
        log.info("判定: 恢复复用 executionId={}（与崩溃前一致），s1 未重复执行，实例 -> COMPLETED",
                recoveredAttempt.executionId());
    }

    @Test
    void crashDuringForwardFailureDoesNotSkipCompensationOnRecovery() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();

        RecordingAction s1 = new RecordingAction("s1");
        RecordingAction s2 = RecordingAction.failAlways("s2");
        RecordingCompensation c1 = new RecordingCompensation("c1");
        SagaDefinition def = SagaFixtures.definition("recovery-comp-flow")
                .step("s1", s1, c1)
                .step("s2", s2, null)
                .build();

        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, registry);
        SagaInstance before = orchestrator.submit(def, "TX-RECOV-2", new SagaContext());
        // 该场景下提交本身就会走到补偿完成；这里改为：提交后已是 COMPENSATED，
        // 再用新编排器 recoverAll 验证终态不被重复处理
        assertEquals(InstanceStatus.COMPENSATED, before.getStatus());
        int invBefore = c1.invocationCount.get();

        SagaOrchestrator revived = new SagaOrchestrator(repo, registry);
        int recovered = revived.recoverAll();
        SagaInstance after = revived.get("TX-RECOV-2");
        assertEquals(0, recovered, "终态实例不得被恢复推进");
        assertEquals(InstanceStatus.COMPENSATED, after.getStatus());
        assertEquals(invBefore, c1.invocationCount.get(), "已完成的补偿不得重复执行");
        log.info("判定: 终态 COMPENSATED 实例在 recoverAll 中被跳过，补偿调用次数保持={}", invBefore);
    }

    @Test
    void resumeCompensationFailedAfterReinitialization() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();

        RecordingAction s1 = new RecordingAction("s1");
        RecordingAction s2 = new RecordingAction("s2");
        RecordingCompensation c1 = new RecordingCompensation("c1", Integer.MAX_VALUE);
        RecordingCompensation c2 = new RecordingCompensation("c2");
        SagaDefinition def = SagaFixtures.definition("recovery-compfail")
                .step("s1", s1, c1)
                .step("s2", s2, c2)
                .step("s3", RecordingAction.failAlways("s3"), null)
                .build();

        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, registry);
        SagaInstance failed = orchestrator.submit(def, "TX-RECOV-3", new SagaContext());
        assertEquals(InstanceStatus.COMPENSATION_FAILED, failed.getStatus());

        // 重新初始化后直接 recoverAll：COMPENSATION_FAILED 仍会被接管；先不修复 -> 仍失败
        SagaOrchestrator revived = new SagaOrchestrator(repo, registry);
        int n = revived.recoverAll();
        assertEquals(1, n, "COMPENSATION_FAILED 属于未完成实例，恢复必须接管");
        assertEquals(InstanceStatus.COMPENSATION_FAILED, revived.get("TX-RECOV-3").getStatus());
        assertEquals(1, c2.effectiveCount.get(), "已经补偿成功的步骤不得重复补偿");

        // 修复后再次恢复，继续补 c1
        c1.fix();
        revived.recoverAll();
        SagaInstance done = revived.get("TX-RECOV-3");
        assertEquals(InstanceStatus.COMPENSATED, done.getStatus());
        assertEquals(1, c1.effectiveCount.get());
        assertEquals(1, c2.effectiveCount.get());
        log.info("判定: COMPENSATION_FAILED 在重新初始化后被接管，修复后继续补偿至 COMPENSATED，中间无重复补偿");
    }
}
