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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 场景十一：并发重复触发补偿收敛到唯一 COMPENSATED 终态，补偿不重复生效。 */
class CompensationConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CompensationConcurrencyTest.class);

    @Test
    void concurrentCompensateRequestsConverge() throws Exception {
        int threads = 12;
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();
        SagaOrchestrator orchestrator = new SagaOrchestrator(repo, registry);

        RecordingCompensation c1 = new RecordingCompensation("c1");
        RecordingCompensation c2 = new RecordingCompensation("c2");
        RecordingCompensation c3 = new RecordingCompensation("c3");
        SagaDefinition def = SagaFixtures.definition("comp-concurrent")
                .step("s1", new RecordingAction("s1"), c1)
                .step("s2", new RecordingAction("s2"), c2)
                .step("s3", new RecordingAction("s3"), c3)
                .build();

        String id = "TX-COMPCONC-1";
        SagaInstance completed = orchestrator.submit(def, id, new SagaContext());
        assertEquals(InstanceStatus.COMPLETED, completed.getStatus());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<InstanceStatus> results = ConcurrentHashMap.newKeySet();
        log.info("输入: {} 线程同时对 COMPLETED 实例触发 compensate", threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(orchestrator.compensate(id).getStatus());
                } catch (Exception e) {
                    log.error("并发补偿异常", e);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        SagaInstance done = orchestrator.get(id);
        log.info("判定: 返回终态集合={}，存储终态={}", results, done.getStatus());
        assertEquals(Set.of(InstanceStatus.COMPENSATED), results);
        assertEquals(InstanceStatus.COMPENSATED, done.getStatus());
        assertEquals(1, c1.effectiveCount.get());
        assertEquals(1, c2.effectiveCount.get());
        assertEquals(1, c3.effectiveCount.get());
        assertEquals(1, c1.invocationCount.get(), "即使并发触发，补偿动作也只允许被调用一次");
        assertEquals(1, c2.invocationCount.get());
        assertEquals(1, c3.invocationCount.get());
        log.info("判定: c1/c2/c3 调用次数均=1、生效次数均=1");
    }
}
