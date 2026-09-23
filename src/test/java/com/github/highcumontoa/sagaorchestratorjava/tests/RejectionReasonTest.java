package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.IllegalSagaTransitionException;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaConcurrentConflictException;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaException;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaNotFoundException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.SagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 场景六：未知实例、非法状态跳转、并发冲突必须以可区分的原因被明确拒绝。 */
class RejectionReasonTest {

    private static final Logger log = LoggerFactory.getLogger(RejectionReasonTest.class);

    private static SagaDefinition simpleFlow() {
        return SagaFixtures.definition("reject-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .build();
    }

    @Test
    void unknownInstanceIsRejectedWithDistinctReason() {
        SagaOrchestrator orchestrator = new SagaOrchestrator(new InMemorySagaRepository(),
                new SagaDefinitionRegistry());

        SagaNotFoundException e1 = assertThrows(SagaNotFoundException.class, () -> orchestrator.get("NOPE"));
        SagaNotFoundException e2 = assertThrows(SagaNotFoundException.class, () -> orchestrator.compensate("NOPE"));
        SagaNotFoundException e3 = assertThrows(SagaNotFoundException.class, () -> orchestrator.getTrace("NOPE"));
        assertEquals(RejectReason.UNKNOWN_INSTANCE, e1.getReason());
        assertEquals(RejectReason.UNKNOWN_INSTANCE, e2.getReason());
        assertEquals(RejectReason.UNKNOWN_INSTANCE, e3.getReason());
        log.info("判定: 未知实例在 get/compensate/trace 上均以 UNKNOWN_INSTANCE 拒绝: {}", e1.getMessage());
    }

    @Test
    void compensatingAgainWhileRunningIsIllegalTransition() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        // 手动构造一个 RUNNING 实例持久化，模拟正向执行中途外部触发补偿
        SagaInstance running = new SagaInstance();
        running.setInstanceId("TX-RUN");
        running.setDefinitionName("reject-flow");
        running.setStatus(InstanceStatus.RUNNING);
        running.setCreatedAt(java.time.Instant.now());
        running.setUpdatedAt(running.getCreatedAt());
        running.getSteps().add(new com.github.highcumontoa.sagaorchestratorjava.saga.model.StepState(0, "s1"));
        repo.insertIfAbsent(running);
        new SagaDefinitionRegistry().register(simpleFlow());
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();
        registry.register(simpleFlow());
        SagaOrchestrator orch2 = new SagaOrchestrator(repo, registry);

        log.info("输入: 对 RUNNING 实例 TX-RUN 触发 compensate（期望拒绝）");
        IllegalSagaTransitionException ex =
                assertThrows(IllegalSagaTransitionException.class, () -> orch2.compensate("TX-RUN"));
        assertEquals(RejectReason.ILLEGAL_TRANSITION, ex.getReason());
        assertTrue(ex.getMessage().contains("RUNNING"));
        log.info("判定: 以 ILLEGAL_TRANSITION 拒绝: {}", ex.getMessage());
    }

    @Test
    void staleVersionUpdateIsRejectedAsConcurrentConflict() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());
        orchestrator.submit(simpleFlow(), "TX-CAS", new SagaContext());

        // 读取快照 v1，然后由“另一个写者”推进存储版本，再用旧快照写 -> CAS 冲突
        SagaInstance stale = repo.find("TX-CAS").orElseThrow();
        long staleVersion = stale.getVersion();
        SagaInstance newer = repo.find("TX-CAS").orElseThrow();
        newer.addTrace("EXTERNAL_WRITE", "模拟另一写者并发推进版本");
        repo.compareAndUpdate(newer, newer.getVersion());
        SagaConcurrentConflictException ex = assertThrows(SagaConcurrentConflictException.class,
                () -> repo.compareAndUpdate(stale, staleVersion));
        assertEquals(RejectReason.CONCURRENT_CONFLICT, ex.getReason());
        log.info("判定: 旧版本写入以 CONCURRENT_CONFLICT 拒绝: {}", ex.getMessage());

        // 三类拒绝原因互不相同
        assertEquals(3, java.util.Set.of(
                RejectReason.UNKNOWN_INSTANCE,
                RejectReason.ILLEGAL_TRANSITION,
                RejectReason.CONCURRENT_CONFLICT).size());
    }

    @Test
    void orchestratorRejectsWhenStorageChangedByAnotherWriter() {
        // 装饰存储：在 orchestrator 第一次 compareAndUpdate 之前把底层版本顶高，
        // 模拟另一写者并发改写，orchestrator 必须抛 CONCURRENT_CONFLICT 而非静默覆盖。
        InMemorySagaRepository backing = new InMemorySagaRepository();
        SagaRepository tampered = new SagaRepository() {
            private boolean armed = true;

            @Override
            public boolean insertIfAbsent(SagaInstance instance) {
                return backing.insertIfAbsent(instance);
            }

            @Override
            public Optional<SagaInstance> find(String instanceId) {
                return backing.find(instanceId);
            }

            @Override
            public void compareAndUpdate(SagaInstance instance, long expectedVersion) {
                if (armed) {
                    armed = false;
                    SagaInstance bump = backing.find(instance.getInstanceId()).orElseThrow();
                    bump.addTrace("EXTERNAL_WRITE", "模拟另一写者并发推进版本");
                    backing.compareAndUpdate(bump, bump.getVersion());
                }
                backing.compareAndUpdate(instance, expectedVersion);
            }

            @Override
            public List<SagaInstance> findAll() {
                return backing.findAll();
            }
        };
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();
        SagaOrchestrator orchestrator = new SagaOrchestrator(tampered, registry);

        SagaException ex = assertThrows(SagaException.class,
                () -> orchestrator.submit(simpleFlow(), "TX-EXT", new SagaContext()));
        assertEquals(RejectReason.CONCURRENT_CONFLICT, ex.getReason());
        log.info("判定: 检测到外部写者后编排器以 CONCURRENT_CONFLICT 中止，未发生静默覆盖: {}", ex.getMessage());
    }
}
