package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 场景四：同一业务实例的重复提交与并发提交收敛到唯一终态，动作不重复生效。 */
class SubmitConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(SubmitConcurrencyTest.class);

    @Test
    void sequentialDuplicateSubmitConverges() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());
        SagaDefinition def = SagaFixtures.definition("dup-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .build();

        String id = "TX-DUP-1";
        SagaInstance first = orchestrator.submit(def, id, new SagaContext());
        SagaInstance second = orchestrator.submit(def, id, new SagaContext());
        SagaInstance third = orchestrator.submit(def, id, new SagaContext());

        log.info("输入: 同一 id={} 连续提交 3 次", id);
        assertEquals(InstanceStatus.COMPLETED, first.getStatus());
        assertEquals(first.getInstanceId(), second.getInstanceId());
        assertEquals(first.getVersion(), second.getVersion(), "重复提交返回同一收敛实例（版本不变）");
        assertEquals(first.getVersion(), third.getVersion());
        SagaInstance stored = orchestrator.get(id);
        assertEquals(InstanceStatus.COMPLETED, stored.getStatus());
        log.info("判定: 三次提交收敛到同一实例，版本号={} 未因重复提交增长", stored.getVersion());
    }

    @Test
    void concurrentSubmitsConvergeToSingleTerminalState() throws Exception {
        int threads = 16;
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, new SagaDefinitionRegistry());

        AtomicInteger entered = new AtomicInteger();
        RecordingAction a1 = new RecordingAction("s1");
        RecordingAction a2 = new RecordingAction("s2", 0, 120L); // 制造执行窗口，增大竞争
        SagaDefinition def = SagaFixtures.definition("concurrent-flow")
                .step("s1", a1, new RecordingCompensation("c1"))
                .step("s2", a2, new RecordingCompensation("c2"))
                .build();

        String id = "TX-CONC-1";
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<InstanceStatus> finalStatuses = ConcurrentHashMap.newKeySet();
        AtomicInteger completedCount = new AtomicInteger();
        log.info("输入: {} 个线程同时 submit 同一 id={}", threads, id);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    entered.incrementAndGet();
                    SagaInstance r = orchestrator.submit(def, id, new SagaContext());
                    finalStatuses.add(r.getStatus());
                    completedCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("并发提交异常", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        log.info("判定: 进入线程={} 完成={} 终态集合={}", entered.get(), completedCount.get(), finalStatuses);
        assertEquals(threads, completedCount.get());
        assertEquals(Set.of(InstanceStatus.COMPLETED), finalStatuses, "所有并发提交必须收敛到 COMPLETED");
        assertEquals(1, a1.effectiveCount.get(), "步骤 s1 只允许生效一次");
        assertEquals(1, a2.effectiveCount.get(), "步骤 s2 只允许生效一次");
        assertEquals(1, repo.find(id).orElseThrow().getTrace().stream()
                .filter(e -> e.eventType().equals("INSTANCE_SUBMITTED")).count(), "只允许创建一次实例");
        log.info("判定: 所有步骤仅生效一次，实例轨迹中仅一条 INSTANCE_SUBMITTED");
    }
}
