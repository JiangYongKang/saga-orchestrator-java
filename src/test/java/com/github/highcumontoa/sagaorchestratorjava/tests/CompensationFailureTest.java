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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：补偿自身失败时不丢失中间状态；
 * 待补偿/补偿中/补偿失败可区分；修复后重复触发补偿必须幂等推进。
 */
class CompensationFailureTest {

    private static final Logger log = LoggerFactory.getLogger(CompensationFailureTest.class);

    @Test
    void compensationFailureKeepsStateAndRetryIsIdempotent() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        RecordingAction a1 = new RecordingAction("step1");
        RecordingAction a2 = new RecordingAction("step2");
        RecordingAction a3 = new RecordingAction("step3");
        RecordingCompensation c1 = new RecordingCompensation("comp1");
        RecordingCompensation c2 = new RecordingCompensation("comp2", Integer.MAX_VALUE); // 永远失败
        RecordingCompensation c3 = new RecordingCompensation("comp3");
        SagaDefinition def = SagaFixtures.definition("order-flow")
                .step("step1", a1, c1)
                .step("step2", a2, c2)
                .step("step3", a3, c3)
                .step("step4", RecordingAction.failAlways("step4"), null)
                .build();

        String id = "TX-COMPFAIL-1";
        log.info("输入: submit instanceId={} comp2 永远失败", id);
        SagaInstance failed = orchestrator.submit(def, id, new SagaContext());

        log.info("判定: 实例状态={} 期望=COMPENSATION_FAILED", failed.getStatus());
        assertEquals(InstanceStatus.COMPENSATION_FAILED, failed.getStatus());
        // step3 已补偿，step2 补偿失败，step1 仍待补偿（中间状态不丢失，且三种状态可区分）
        assertEquals(StepStatus.COMPENSATED, failed.getSteps().get(2).getStatus());
        assertEquals(StepStatus.COMPENSATION_FAILED, failed.getSteps().get(1).getStatus());
        assertEquals(StepStatus.COMPENSATION_PENDING, failed.getSteps().get(0).getStatus());
        assertEquals(1, c3.effectiveCount.get());
        assertEquals(0, c2.effectiveCount.get());
        assertEquals(0, c1.invocationCount.get(), "更早日补偿步骤必须保持未执行");
        assertTrue(failed.getLastFailureReason().contains("step2"), "需要记录失败步骤: " + failed.getLastFailureReason());

        // 修复 comp2：此后重试补偿应继续推进，且 comp3 不得重复生效
        c2.fix();
        log.info("输入: 再次触发 compensate（comp2 已修复），期望继续补偿且已完成补偿不重复");
        SagaInstance retried = orchestrator.compensate(id);
        assertEquals(InstanceStatus.COMPENSATED, retried.getStatus());
        assertEquals(1, c3.effectiveCount.get(), "已补偿的 step3 不得重复补偿");
        assertEquals(1, c2.effectiveCount.get());
        assertEquals(1, c1.effectiveCount.get());

        // 再次重复触发：终态幂等（记录调用次数，之后不再增长）
        int inv1 = c1.invocationCount.get();
        int inv2 = c2.invocationCount.get();
        int inv3 = c3.invocationCount.get();
        SagaInstance again = orchestrator.compensate(id);
        assertEquals(InstanceStatus.COMPENSATED, again.getStatus());
        assertEquals(inv1, c1.invocationCount.get(), "COMPENSATED 终态重复触发不得再调用 comp1");
        assertEquals(inv2, c2.invocationCount.get(), "COMPENSATED 终态重复触发不得再调用 comp2");
        assertEquals(inv3, c3.invocationCount.get(), "COMPENSATED 终态重复触发不得再调用 comp3");
        // 失败后重试属于新的尝试（invocation 允许 2 次），但实际生效始终只有一次
        assertEquals(1, c2.effectiveCount.get());
        log.info("判定: 终态重复补偿调用次数不再增长(c1={},c2={},c3={})，各动作生效次数均=1", inv1, inv2, inv3);
        log.info("最终轨迹:\n{}", String.join("\n",
                again.getTrace().stream().map(t -> "  " + t.seq() + " " + t.detail()).toList()));
    }
}
