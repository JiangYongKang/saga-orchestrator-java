package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepExecution;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceType;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.GuardRecord;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.IdempotentActionGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在途崩溃恢复测试：动作已下发、结果未确认时进程崩溃，重启后必须收敛到唯一终态——
 * 已生效的业务操作不重复生效，未完成的补偿不被跳过；重复/并发恢复保持幂等。
 *
 * <p>崩溃模拟方式：动作在副作用落进「业务系统」后永久挂起（等同于 JVM 在确认落盘前被杀），
 * 随后弃用该编排器进程，在同一存储目录上换新编排器恢复。
 */
class InFlightCrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(InFlightCrashRecoveryTest.class);

    @TempDir
    Path tempDir;

    /**
     * 模拟业务侧系统：以 dedupKey 为幂等键，同一键只真正生效一次；
     * 记录每次调用的入参，供断言「重放用的是同一个键」。
     */
    static final class BusinessSystem {
        private final ConcurrentHashMap<String, Boolean> applied = new ConcurrentHashMap<>();
        private final AtomicInteger realEffects = new AtomicInteger();
        private final List<String> invocationKeys = new CopyOnWriteArrayList<>();

        /** @return true=本次真正生效；false=识别为重放，挡掉重复 */
        boolean apply(String dedupKey, String effectName) {
            invocationKeys.add(dedupKey);
            boolean first = applied.putIfAbsent(dedupKey, Boolean.TRUE) == null;
            if (first) {
                realEffects.incrementAndGet();
                log.info("[business] effect '{}' APPLIED dedupKey={}", effectName, dedupKey);
            } else {
                log.info("[business] duplicate replay SUPPRESSED dedupKey={} (effect '{}' already applied)",
                        dedupKey, effectName);
            }
            return first;
        }

        int realEffects() {
            return realEffects.get();
        }

        int invocations() {
            return invocationKeys.size();
        }

        List<String> invocationKeys() {
            return invocationKeys;
        }
    }

    private SagaOrchestrator newOrchestrator() {
        return new SagaOrchestrator(new FileSagaStore(tempDir));
    }

    private static Thread daemon(Runnable r) {
        Thread t = new Thread(r, "crashed-process-simulator");
        t.setDaemon(true);
        return t;
    }

    // 1. 正向链路：副作用已生效、确认未落盘时崩溃 ----------------------------

    @Test
    void forwardInFlightCrashRecoversToUniqueTerminalState() {
        String type = "crash-forward";
        String inst = "inst-crash-fwd";
        BusinessSystem business = new BusinessSystem();
        CountDownLatch effectApplied = new CountDownLatch(1);
        CountDownLatch hangForever = new CountDownLatch(1); // 永不释放 = 进程崩溃

        // 进程 A：动作先生效，随后「崩溃」（挂起，确认永远不落盘）
        SagaOrchestrator processA = newOrchestrator();
        processA.register(new SagaDefinition(type, List.of(
                new StepDefinition("charge", ctx -> {
                    business.apply(ctx.dedupKey(), "charge");
                    effectApplied.countDown();
                    log.info("[process-A] effect applied, simulating crash before confirm (hang)");
                    hangForever.await();
                    return "UNREACHABLE";
                }, null, 1, 0L, 0L))));
        daemon(() -> processA.submit(type, inst, "BIZ-F", Map.of())).start();

        try {
            assertTrue(await(effectApplied), "前置：崩溃前副作用必须已生效");
            assertEquals(1, business.realEffects());

            // 进程 B：同一存储目录。先查快照——必须能直接看出在途中间态
            SagaOrchestrator processB = newOrchestrator();
            SagaSnapshot duringCrash = processB.snapshot(inst);
            SagaSnapshot.StepView stepView = duringCrash.steps().get(0);
            log.info("[assert] mid-crash view: instanceStatus={} stepStatus={} inFlightPhase={} forwardDedupKey={}",
                    duringCrash.status(), stepView.status(), stepView.inFlightPhase(), stepView.forwardDedupKey());
            assertEquals(SagaStatus.RUNNING, duringCrash.status());
            assertEquals(StepStatus.RUNNING, stepView.status());
            assertEquals("FORWARD", stepView.inFlightPhase(),
                    "判定依据：快照必须直接暴露「动作已下发、结果未确认」的在途态");
            assertEquals(IdempotentActionGuard.key(inst, "charge", "FORWARD"), stepView.forwardDedupKey());

            // 定义未注册时恢复不动该实例（顺序无关性的前半截）
            assertEquals(0, processB.recoverAll(), "判定依据：定义缺失时恢复必须跳过而不是误推进");
            assertEquals(SagaStatus.RUNNING, processB.snapshot(inst).status());

            // 注册定义（=定义加载晚于恢复尝试）：注册动作本身应补恢复，不能漏
            processB.register(new SagaDefinition(type, List.of(
                    new StepDefinition("charge", ctx -> {
                        business.apply(ctx.dedupKey(), "charge");
                        return "REPLAYED-OK";
                    }, null, 1, 0L, 0L))));

            SagaSnapshot after = processB.snapshot(inst);
            log.info("[assert] recovered: status={} realEffects={} invocations={} keys={}",
                    after.status(), business.realEffects(), business.invocations(), business.invocationKeys());
            assertEquals(SagaStatus.COMPLETED, after.status(), "判定依据：恢复后收敛到唯一终态 COMPLETED");
            assertEquals(1, business.realEffects(),
                    "判定依据：崩溃前已生效的业务操作不得再生效应第二次");
            assertEquals(2, business.invocations(), "判定依据：结果未知必须重放一次（由业务侧判重）");
            assertEquals(business.invocationKeys().get(0), business.invocationKeys().get(1),
                    "判定依据：跨崩溃重放的去重标识必须与首次完全一致");

            // 轨迹必须解释在途处置依据
            assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED
                            && t.getDetail().contains(stepView.forwardDedupKey())),
                    "判定依据：轨迹必须记录「发现在途记录 -> 以同一 dedupKey 重放」的处置");
            assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.RECOVERED));

            // 恢复后的快照不允许矛盾结论：终态 + 步骤成功 + 无在途
            SagaSnapshot.StepView doneView = after.steps().get(0);
            assertEquals(StepStatus.SUCCEEDED, doneView.status());
            assertNull(doneView.inFlightPhase(), "判定依据：收敛后不得再报告在途中间态");

            // 自动恢复之后再手动恢复：幂等 no-op，不产生新副作用
            assertEquals(0, processB.recoverAll());
            assertEquals(0, processB.recoverAll());
            assertEquals(1, business.realEffects());
            assertEquals(2, business.invocations(), "判定依据：重复恢复不得触发任何新的业务调用");
            assertEquals(SagaStatus.COMPLETED, processB.snapshot(inst).status());
        } finally {
            hangForever.countDown(); // 放行模拟崩溃的线程，避免泄漏
        }
    }

    // 2. 补偿链路：补偿已生效、确认未落盘时崩溃 ------------------------------

    @Test
    void compensationInFlightCrashIsReplayedNotSkipped() {
        String type = "crash-compensate";
        String inst = "inst-crash-comp";
        BusinessSystem business = new BusinessSystem();
        CountDownLatch compEffectApplied = new CountDownLatch(1);
        CountDownLatch hangForever = new CountDownLatch(1);

        SagaOrchestrator processA = newOrchestrator();
        processA.register(new SagaDefinition(type, List.of(
                new StepDefinition("reserve",
                        ctx -> "RESERVED",
                        ctx -> {
                            business.apply(ctx.dedupKey(), "release-reserve");
                            compEffectApplied.countDown();
                            log.info("[process-A] compensation effect applied, simulating crash (hang)");
                            hangForever.await();
                        },
                        1, 0L, 0L),
                new StepDefinition("pay", ctx -> {
                    throw new IllegalStateException("card declined");
                }, null, 1, 0L, 0L))));
        daemon(() -> processA.submit(type, inst, "BIZ-C", Map.of())).start();

        try {
            assertTrue(await(compEffectApplied), "前置：崩溃前补偿副作用必须已生效");
            assertEquals(1, business.realEffects());

            SagaOrchestrator processB = newOrchestrator();
            SagaSnapshot duringCrash = processB.snapshot(inst);
            log.info("[assert] mid-crash: status={} step0={} inFlight={}",
                    duringCrash.status(), duringCrash.steps().get(0).status(),
                    duringCrash.steps().get(0).inFlightPhase());
            assertEquals(SagaStatus.COMPENSATING, duringCrash.status());
            assertEquals("COMPENSATE", duringCrash.steps().get(0).inFlightPhase(),
                    "判定依据：补偿在途态同样必须可见");
            String compKey = duringCrash.steps().get(0).compensationDedupKey();
            assertEquals(IdempotentActionGuard.key(inst, "reserve", "COMPENSATE"), compKey);

            processB.register(new SagaDefinition(type, List.of(
                    new StepDefinition("reserve",
                            ctx -> "RESERVED",
                            ctx -> business.apply(ctx.dedupKey(), "release-reserve"),
                            1, 0L, 0L),
                    new StepDefinition("pay", ctx -> "PAID", null, 1, 0L, 0L))));
            int recovered = processB.recoverAll();

            SagaSnapshot after = processB.snapshot(inst);
            log.info("[assert] recovered={} status={} realEffects={} keys={}",
                    recovered, after.status(), business.realEffects(), business.invocationKeys());
            assertTrue(recovered <= 1, "注册时可能已补恢复，recoverAll 为幂等");
            assertEquals(SagaStatus.COMPENSATED, after.status(),
                    "判定依据：补偿在途崩溃后必须继续补偿到终态，不得卡在 COMPENSATING");
            assertEquals(1, business.realEffects(),
                    "判定依据：已生效的补偿不得重复生效，也不得被跳过");
            assertEquals(2, business.invocations());
            assertEquals(business.invocationKeys().get(0), business.invocationKeys().get(1));
            assertEquals(compKey, business.invocationKeys().get(1),
                    "判定依据：补偿重放必须使用同一个稳定去重标识");
            assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED
                    && "reserve".equals(t.getStepName())));
        } finally {
            hangForever.countDown();
        }
    }

    // 3. 确认已落盘、实例状态未落盘时崩溃：采用守卫结论，绝不重放 ------------

    @Test
    void confirmedButUnpersistedEffectIsAdoptedWithoutReexecution() {
        String type = "adopt-flow";
        String inst = "inst-crash-adopt";
        // 白盒重建崩溃现场：守卫 DONE（确认已落盘），实例仍 RUNNING（状态没来得及落盘）
        FileSagaStore store = new FileSagaStore(tempDir);
        long now = System.currentTimeMillis();
        SagaInstance crashed = new SagaInstance();
        crashed.setInstanceId(inst);
        crashed.setType(type);
        crashed.setBusinessId("BIZ-A");
        crashed.setStatus(SagaStatus.RUNNING);
        crashed.setCreatedMillis(now);
        crashed.setUpdatedMillis(now);
        StepExecution se = new StepExecution();
        se.setName("charge");
        se.setIndex(0);
        se.setStatus(StepStatus.RUNNING);
        se.setUpdatedMillis(now);
        crashed.setSteps(new ArrayList<>(List.of(se)));
        store.createIfAbsent(crashed);
        GuardRecord done = new GuardRecord();
        done.setKey(IdempotentActionGuard.key(inst, "charge", "FORWARD"));
        done.setState(IdempotentActionGuard.DONE);
        done.setAttempt(1);
        done.setResultJson("\"CONFIRMED-BEFORE-CRASH\"");
        done.setUpdatedMillis(now);
        store.saveGuard(done);
        log.info("[setup] crash state rebuilt: guard=DONE(attempt=1) but step=RUNNING, instance=RUNNING");

        AtomicInteger actionRuns = new AtomicInteger();
        SagaOrchestrator processB = newOrchestrator();
        processB.register(new SagaDefinition(type, List.of(
                new StepDefinition("charge", ctx -> {
                    actionRuns.incrementAndGet();
                    return "MUST-NOT-RUN";
                }, null, 1, 0L, 0L))));
        processB.recoverAll();

        SagaSnapshot after = processB.snapshot(inst);
        log.info("[assert] status={} stepResult={} actionRuns={}",
                after.status(), after.steps().get(0).result(), actionRuns.get());
        assertEquals(SagaStatus.COMPLETED, after.status());
        assertEquals(0, actionRuns.get(),
                "判定依据：守卫已 DONE 的动作确认不得重放，直接采用已确认结论");
        assertEquals("CONFIRMED-BEFORE-CRASH", after.steps().get(0).result(),
                "判定依据：步骤结果必须来自守卫中已确认的结果，保证状态结论不矛盾");
        assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.STEP_SUCCEEDED
                        && t.getDetail().contains("already confirmed")),
                "判定依据：轨迹必须解释「采用已确认结论」这一收敛依据");
    }

    // 4. 并发 + 重复恢复：多进程同时恢复同一崩溃现场，结论一致 --------------

    @Test
    void concurrentAndRepeatedRecoveryStaysIdempotent() throws Exception {
        String type = "crash-concurrent";
        String inst = "inst-crash-conc";
        BusinessSystem business = new BusinessSystem();
        CountDownLatch effectApplied = new CountDownLatch(1);
        CountDownLatch hangForever = new CountDownLatch(1);

        // 两个「新进程」先就位（此时还没有任何实例，注册不会触发恢复）
        SagaOrchestrator processB = newOrchestrator();
        SagaOrchestrator processC = newOrchestrator();
        StepDefinition def = new StepDefinition("charge", ctx -> {
            business.apply(ctx.dedupKey(), "charge");
            return "OK";
        }, null, 1, 0L, 0L);
        processB.register(new SagaDefinition(type, List.of(def)));
        processC.register(new SagaDefinition(type, List.of(def)));

        // 进程 A 制造崩溃现场
        SagaOrchestrator processA = newOrchestrator();
        processA.register(new SagaDefinition(type, List.of(
                new StepDefinition("charge", ctx -> {
                    business.apply(ctx.dedupKey(), "charge");
                    effectApplied.countDown();
                    hangForever.await();
                    return "UNREACHABLE";
                }, null, 1, 0L, 0L))));
        daemon(() -> processA.submit(type, inst, "BIZ-X", Map.of())).start();

        try {
            assertTrue(await(effectApplied));

            // B、C 多线程并发恢复 + 恢复完成后再重复恢复
            ExecutorService pool = Executors.newFixedThreadPool(6);
            CountDownLatch start = new CountDownLatch(1);
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 6; i++) {
                SagaOrchestrator target = i % 2 == 0 ? processB : processC;
                pool.submit(() -> {
                    try {
                        start.await();
                        target.recoverAll();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            assertTrue(errors.isEmpty(), "并发恢复不得以异常失败: " + errors);

            int secondWave = processB.recoverAll() + processC.recoverAll();
            SagaSnapshot fromB = processB.snapshot(inst);
            SagaSnapshot fromC = processC.snapshot(inst);
            log.info("[assert] secondWaveRecovered={} statusB={} statusC={} realEffects={} invocations={}",
                    secondWave, fromB.status(), fromC.status(), business.realEffects(), business.invocations());

            assertEquals(0, secondWave, "判定依据：收敛后的重复恢复必须是幂等 no-op");
            assertEquals(SagaStatus.COMPLETED, fromB.status());
            assertEquals(SagaStatus.COMPLETED, fromC.status(),
                    "判定依据：多个恢复者必须观察到同一个不矛盾的终态");
            assertEquals(1, business.realEffects(),
                    "判定依据：并发恢复下业务副作用仍然只生效一次");
            assertTrue(business.invocationKeys().stream().allMatch(k -> k.equals(business.invocationKeys().get(0))),
                    "判定依据：所有恢复者重放的去重标识必须一致");
        } finally {
            hangForever.countDown();
        }
    }

    // 5. 去重标识：跨重试稳定、跨动作不撞车 ----------------------------------

    @Test
    void dedupKeyIsStableAcrossRetriesAndDistinctAcrossActions() {
        String type = "dedup-key-flow";
        String inst = "inst-dedup";
        List<String> stepAKeys = new CopyOnWriteArrayList<>();
        List<String> stepBKeys = new CopyOnWriteArrayList<>();
        List<String> compAKeys = new CopyOnWriteArrayList<>();
        AtomicInteger aRuns = new AtomicInteger();

        SagaOrchestrator orch = newOrchestrator();
        orch.register(new SagaDefinition(type, List.of(
                new StepDefinition("a", ctx -> {
                    stepAKeys.add(ctx.dedupKey());
                    if (aRuns.incrementAndGet() < 3) {
                        throw new IllegalStateException("transient");
                    }
                    return "A-OK";
                }, ctx -> compAKeys.add(ctx.dedupKey()), 3, 0L, 1L),
                new StepDefinition("b", ctx -> {
                    stepBKeys.add(ctx.dedupKey());
                    throw new IllegalStateException("boom"); // 触发对 a 的补偿
                }, null, 1, 0L, 0L))));

        SagaSnapshot snap = orch.submit(type, inst, "BIZ-D", Map.of()).snapshot();
        log.info("[assert] status={} aKeys={} bKeys={} compAKeys={}",
                snap.status(), stepAKeys, stepBKeys, compAKeys);

        assertEquals(SagaStatus.COMPENSATED, snap.status());
        assertEquals(3, stepAKeys.size(), "前置：a 重试了 3 次");
        assertEquals(1, stepAKeys.stream().distinct().count(),
                "判定依据：同一逻辑动作重试多次，去重标识不变");
        assertEquals(IdempotentActionGuard.key(inst, "a", "FORWARD"), stepAKeys.get(0));
        assertEquals(List.of(IdempotentActionGuard.key(inst, "b", "FORWARD")), stepBKeys);
        assertEquals(List.of(IdempotentActionGuard.key(inst, "a", "COMPENSATE")), compAKeys);
        assertNotEquals(stepAKeys.get(0), stepBKeys.get(0), "判定依据：不同动作的标识不得撞车");
        assertNotEquals(stepAKeys.get(0), compAKeys.get(0), "判定依据：同一步骤正向与补偿的标识不得撞车");

        // 重启后同一实例同一动作的标识仍一致（确定性推导，无需迁移老数据）
        SagaOrchestrator restarted = newOrchestrator();
        SagaSnapshot view = restarted.snapshot(inst);
        assertEquals(stepAKeys.get(0), view.steps().get(0).forwardDedupKey(),
                "判定依据：跨重启去重标识保持稳定（对老实例同样成立）");
        assertEquals(compAKeys.get(0), view.steps().get(0).compensationDedupKey());
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
