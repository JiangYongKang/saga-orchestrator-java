package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.ActionKeys;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceType;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在途崩溃恢复测试：动作已下发、结果未确认时进程被杀/重启，
 * 重启后必须收敛到唯一终态——已生效的业务操作不得第二次生效（业务按稳定 actionKey 判重），
 * 未完成的补偿不得被跳过；并发/重复恢复保持幂等。
 *
 * <p>每个用例的日志都会打印输入、状态迁移与判定依据。
 */
class SagaCrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(SagaCrashRecoveryTest.class);

    @TempDir
    Path tempDir;

    private SagaOrchestrator newOrchestrator() {
        return new SagaOrchestrator(new FileSagaStore(tempDir));
    }

    /**
     * 模拟业务侧：以稳定 actionKey 判重的「业务账本」。
     * 同一个 key 无论被重放多少次，真正的副作用只生效一次。
     */
    static final class BusinessLedger {
        private final Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
        private final List<String> invocations = new CopyOnWriteArrayList<>();
        private final AtomicInteger sideEffects = new AtomicInteger();

        /** 业务动作模板：编排器每次（重）调用都会进来；只有首次见到 key 才真正生效。 */
        void applyOnce(String actionKey, String what) {
            invocations.add(actionKey);
            if (appliedKeys.add(actionKey)) {
                sideEffects.incrementAndGet();
                log.info("[business] APPLY {} key={} (first sight -> side effect #{})",
                        what, actionKey, sideEffects.get());
            } else {
                log.info("[business] SUPPRESS {} key={} (duplicate replay of same logical action)", what, actionKey);
            }
        }

        int sideEffects() {
            return sideEffects.get();
        }

        List<String> invocations() {
            return invocations;
        }
    }

    /** 在后台线程提交（动作会阻塞，模拟进程在确认落盘前被杀）。 */
    private static Thread submitInBackground(SagaOrchestrator orch, String type, String instanceId) {
        Thread t = new Thread(() -> {
            try {
                orch.submit(type, instanceId, "BIZ-" + instanceId, Map.of());
            } catch (RuntimeException e) {
                // 旧进程「尸体」的迟到落盘会被乐观版本拒绝，属预期
                log.info("[crash-sim] late write from killed process rejected as expected: {}", e.getMessage());
            }
        }, "crash-sim-submitter");
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** 等待某步进入「在途」中间态（动作已下发、确认未落盘）。 */
    private static SagaSnapshot.StepView awaitInFlight(SagaOrchestrator orch, String instanceId,
                                                       String stepName) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < deadline) {
            SagaSnapshot snap;
            try {
                snap = orch.snapshot(instanceId);
            } catch (RuntimeException notCreatedYet) {
                Thread.sleep(10);
                continue;
            }
            SagaSnapshot.StepView view = snap.steps().stream()
                    .filter(s -> s.name().equals(stepName)).findFirst().orElseThrow();
            if (view.inFlight()) {
                log.info("[assert] in-flight visible in snapshot: step={} status={} attempt={} actionKey={}",
                        stepName, view.status(), view.inFlightAttempt(), view.actionKey());
                return view;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("step " + stepName + " never became in-flight");
    }

    // 1. 正向动作在途崩溃：已生效不重复、收敛唯一终态、恢复快照可解释 ----------

    @Test
    void forwardActionCrashMidFlightRecoversWithoutDoubleEffect() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);
        String expectedKey = ActionKeys.forward("crash-flow", "inst-c1", "debit");

        // 第一个进程：动作生效后、确认落盘前「崩溃」（动作线程永远卡住，编排器被丢弃）
        SagaOrchestrator killed = newOrchestrator();
        killed.register(new SagaDefinition("crash-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    log.info("[input] debit dispatched with actionKey={} attempt={}", ctx.actionKey(), ctx.attempt());
                    try {
                        blockKilledProcess.await(30, TimeUnit.SECONDS); // 确认永远来不及落盘
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "DEBITED";
                }, null, 1, 0L, 0L))));
        submitInBackground(killed, "crash-flow", "inst-c1");

        SagaSnapshot.StepView inFlight = awaitInFlight(killed, "inst-c1", "debit");
        assertEquals(StepStatus.RUNNING, inFlight.status());
        assertEquals(expectedKey, inFlight.actionKey(),
                "判定依据：快照必须暴露稳定去重标识，且派生规则与 ActionKeys 一致");
        assertEquals(1, inFlight.inFlightAttempt());
        assertEquals(1, ledger.sideEffects());
        log.info("[crash-sim] process killed while guard=IN_FLIGHT, effect applied once, confirmation lost");

        // 重启：同目录新编排器 + 同名定义（业务实现仍按 actionKey 判重）
        SagaOrchestrator restarted = newOrchestrator();
        restarted.register(new SagaDefinition("crash-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    log.info("[input] debit REPLAYED after restart with actionKey={}", ctx.actionKey());
                    return "DEBITED";
                }, null, 1, 0L, 0L))));

        int recovered = restarted.recoverAll();
        SagaSnapshot after = restarted.snapshot("inst-c1");
        log.info("[assert] recovered={} finalStatus={} sideEffects={} invocations={}",
                recovered, after.status(), ledger.sideEffects(), ledger.invocations());

        assertEquals(1, recovered, "判定依据：恰好一个在途实例被恢复");
        assertEquals(SagaStatus.COMPLETED, after.status(), "判定依据：恢复后收敛到唯一终态 COMPLETED");
        assertEquals(1, ledger.sideEffects(),
                "判定依据：业务按稳定 actionKey 判重，已生效的 debit 不得第二次生效");
        assertEquals(2, ledger.invocations().size(), "判定依据：编排器确实重放了该动作（结果未知必须重放）");
        assertEquals(expectedKey, ledger.invocations().get(0));
        assertEquals(expectedKey, ledger.invocations().get(1),
                "判定依据：跨重启重放的标识与首次完全一致");
        assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED),
                "判定依据：轨迹必须解释在途动作的处置方式");
        assertFalse(after.steps().get(0).inFlight(), "判定依据：终态快照不允许残留矛盾的在途标记");

        // 自动恢复之后再手动驱动一次：幂等 no-op
        int again = restarted.recoverAll();
        log.info("[assert] manual recoverAll after auto recovery -> resumed={}", again);
        assertEquals(0, again, "判定依据：终态实例的重复恢复是幂等 no-op");
        assertEquals(1, ledger.sideEffects());
        assertEquals(SagaStatus.COMPLETED, restarted.snapshot("inst-c1").status());

        blockKilledProcess.countDown(); // 释放「尸体」线程，其迟到写将被乐观版本/守卫拒绝
    }

    // 2. 补偿动作在途崩溃：补偿不重复生效、也不因重启被跳过 ------------------

    @Test
    void compensationCrashMidFlightIsNeitherSkippedNorDoubled() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);
        String forwardKey = ActionKeys.forward("comp-crash-flow", "inst-c2", "reserve");
        String compKey = ActionKeys.compensation("comp-crash-flow", "inst-c2", "reserve");

        SagaOrchestrator killed = newOrchestrator();
        killed.register(new SagaDefinition("comp-crash-flow", List.of(
                new StepDefinition("reserve",
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "reserve");
                            return "RESERVED";
                        },
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "release");
                            log.info("[input] release dispatched with actionKey={}", ctx.actionKey());
                            try {
                                blockKilledProcess.await(30, TimeUnit.SECONDS); // 补偿确认前「崩溃」
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        1, 0L, 0L),
                new StepDefinition("charge", ctx -> {
                    throw new IllegalStateException("card declined");
                }, null, 1, 0L, 0L))));
        submitInBackground(killed, "comp-crash-flow", "inst-c2");

        SagaSnapshot.StepView inFlight = awaitInFlight(killed, "inst-c2", "reserve");
        assertEquals(StepStatus.COMPENSATING, inFlight.status());
        assertEquals(compKey, inFlight.compensationKey(),
                "判定依据：补偿动作的稳定标识与正向不同，且派生规则一致");
        assertNotEquals(forwardKey, compKey, "判定依据：同一步骤的正向与补偿标识不得撞车");
        SagaSnapshot crashed = killed.snapshot("inst-c2");
        assertEquals(SagaStatus.COMPENSATING, crashed.status());
        log.info("[crash-sim] process killed during compensation, guard=IN_FLIGHT, status={}", crashed.status());

        // 重启恢复：补偿必须被重放（不跳过），但业务按键判重（不重复生效）
        SagaOrchestrator restarted = newOrchestrator();
        restarted.register(new SagaDefinition("comp-crash-flow", List.of(
                new StepDefinition("reserve",
                        ctx -> "RESERVED",
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "release");
                            log.info("[input] release REPLAYED after restart with actionKey={}", ctx.actionKey());
                        },
                        1, 0L, 0L),
                new StepDefinition("charge", ctx -> "X", null, 1, 0L, 0L))));

        int recovered = restarted.recoverAll();
        SagaSnapshot after = restarted.snapshot("inst-c2");
        log.info("[assert] recovered={} finalStatus={} sideEffects={} invocations={}",
                recovered, after.status(), ledger.sideEffects(), ledger.invocations());

        assertEquals(1, recovered);
        assertEquals(SagaStatus.COMPENSATED, after.status(),
                "判定依据：在途补偿崩溃后必须继续补偿到终态，不得因重启被跳过");
        assertEquals(2, ledger.sideEffects(),
                "判定依据：正向生效一次 + 补偿生效一次，两者都不得重复");
        assertEquals(2, ledger.invocations().stream().filter(compKey::equals).count(),
                "判定依据：补偿被重放一次（崩溃前一次 + 恢复后一次），且标识不变");
        assertEquals(1, ledger.invocations().stream().filter(forwardKey::equals).count(),
                "判定依据：已确认成功的正向步骤在恢复中绝不重放");
        assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED
                        && t.getDetail().contains("COMPENSATE")),
                "判定依据：轨迹必须记录对在途补偿的处置");
        assertEquals(StepStatus.COMPENSATED, after.steps().get(0).status());
        assertEquals(StepStatus.FAILED, after.steps().get(1).status());

        blockKilledProcess.countDown();
    }

    // 3. 恢复扫描先于定义注册：定义补齐后自动补恢复，不漏实例 ----------------

    @Test
    void recoveryScanBeforeDefinitionRegistrationStillRecovers() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);

        SagaOrchestrator killed = newOrchestrator();
        killed.register(new SagaDefinition("late-def-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    try {
                        blockKilledProcess.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "DEBITED";
                }, null, 1, 0L, 0L))));
        submitInBackground(killed, "late-def-flow", "inst-c3");
        awaitInFlight(killed, "inst-c3", "debit");
        log.info("[crash-sim] process killed with in-flight action; restarting WITHOUT definition first");

        // 重启后先跑自动恢复（模拟 SagaStartupRecovery 先于业务定义加载）
        SagaOrchestrator restarted = newOrchestrator();
        int scanned = restarted.recoverAll();
        SagaSnapshot pending = restarted.snapshot("inst-c3");
        log.info("[assert] recoverAll without definition -> resumed={} status={} inFlight={}",
                scanned, pending.status(), pending.steps().get(0).inFlight());
        assertEquals(0, scanned, "判定依据：定义未注册时恢复扫描不得推进、也不得丢弃该实例");
        assertEquals(SagaStatus.RUNNING, pending.status());
        assertTrue(pending.steps().get(0).inFlight(),
                "判定依据：在途中间态在恢复扫描后依然如实可见，不被误判为终态");

        // 定义随后注册：必须自动补恢复，无需再次手动 recoverAll
        restarted.register(new SagaDefinition("late-def-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    log.info("[input] debit replayed by deferred recovery, key={}", ctx.actionKey());
                    return "DEBITED";
                }, null, 1, 0L, 0L))));

        SagaSnapshot after = restarted.snapshot("inst-c3");
        log.info("[assert] after late register: status={} sideEffects={}", after.status(), ledger.sideEffects());
        assertEquals(SagaStatus.COMPLETED, after.status(),
                "判定依据：定义补齐后必须自动补恢复，不能漏掉任何应继续推进的实例");
        assertEquals(1, ledger.sideEffects());
        assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED));

        blockKilledProcess.countDown();
    }

    // 4. 并发 + 重复恢复：结论一致、副作用不增多 ----------------------------

    @Test
    void concurrentAndRepeatedRecoveryStaysIdempotent() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);

        SagaOrchestrator killed = newOrchestrator();
        killed.register(new SagaDefinition("conc-recover-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    try {
                        blockKilledProcess.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "DEBITED";
                }, null, 1, 0L, 0L))));
        submitInBackground(killed, "conc-recover-flow", "inst-c4");
        awaitInFlight(killed, "inst-c4", "debit");

        SagaOrchestrator restarted = newOrchestrator();
        restarted.register(new SagaDefinition("conc-recover-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    return "DEBITED";
                }, null, 1, 0L, 0L))));

        int threads = 6;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Integer> recoveredCounts = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                recoveredCounts.add(restarted.recoverAll());
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        // 并发恢复之后再来一次手动恢复
        int manual = restarted.recoverAll();
        SagaSnapshot after = restarted.snapshot("inst-c4");
        long recoveredTraces = after.traces().stream()
                .filter(t -> t.getType() == TraceType.RECOVERED).count();
        long inFlightTraces = after.traces().stream()
                .filter(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED).count();
        log.info("[assert] recoveredCounts={} manual={} finalStatus={} sideEffects={} "
                        + "RECOVERED traces={} IN_FLIGHT_RECOVERED traces={}",
                recoveredCounts, manual, after.status(), ledger.sideEffects(),
                recoveredTraces, inFlightTraces);

        assertEquals(threads, recoveredCounts.size());
        assertEquals(1, recoveredCounts.stream().filter(c -> c == 1).count(),
                "判定依据：并发恢复中恰好一个真正推进实例，其余在锁内重读后幂等收敛");
        assertEquals(0, manual, "判定依据：恢复之后再手动恢复是幂等 no-op");
        assertEquals(SagaStatus.COMPLETED, after.status(),
                "判定依据：并发/重复恢复后收敛到唯一终态，不存在矛盾结论");
        assertEquals(1, ledger.sideEffects(), "判定依据：并发恢复不得让副作用多出生效");
        assertEquals(1, recoveredTraces, "判定依据：轨迹中只允许出现一次恢复结论，不自相矛盾");
        assertEquals(1, inFlightTraces);
        assertEquals(2, ledger.invocations().size());

        blockKilledProcess.countDown();
    }

    // 5. 稳定去重标识：跨重试不变、不同动作互不撞车 --------------------------

    @Test
    void actionKeyIsStableAcrossRetriesAndUniquePerAction() {
        SagaOrchestrator orch = newOrchestrator();
        List<String> flakyKeys = new CopyOnWriteArrayList<>();
        List<String> otherKeys = new CopyOnWriteArrayList<>();
        AtomicInteger flakyRuns = new AtomicInteger();

        orch.register(new SagaDefinition("key-flow", List.of(
                new StepDefinition("flaky", ctx -> {
                    flakyKeys.add(ctx.actionKey());
                    log.info("[input] flaky attempt={} actionKey={}", ctx.attempt(), ctx.actionKey());
                    if (flakyRuns.incrementAndGet() < 3) {
                        throw new IllegalStateException("transient");
                    }
                    return "OK";
                }, null, 3, 0L, 1L),
                new StepDefinition("other", ctx -> {
                    otherKeys.add(ctx.actionKey());
                    return "OK";
                }, null, 1, 0L, 0L))));

        SagaSnapshot snap = orch.submit("key-flow", "inst-c5", "BIZ-C5", Map.of()).snapshot();
        log.info("[assert] flakyKeys={} otherKeys={}", flakyKeys, otherKeys);

        assertEquals(SagaStatus.COMPLETED, snap.status());
        assertEquals(3, flakyKeys.size(), "判定依据：失败重试两次后成功，共 3 次调用");
        assertEquals(1, flakyKeys.stream().distinct().count(),
                "判定依据：同一逻辑动作无论重试多少次，去重标识不变");
        assertEquals(ActionKeys.forward("key-flow", "inst-c5", "flaky"), flakyKeys.get(0),
                "判定依据：标识派生规则对外公开、可预测");
        assertNotEquals(flakyKeys.get(0), otherKeys.get(0),
                "判定依据：不同步骤的标识不得撞车");
        assertNotEquals(ActionKeys.forward("key-flow", "inst-c5", "flaky"),
                ActionKeys.compensation("key-flow", "inst-c5", "flaky"),
                "判定依据：同一步骤的正向与补偿标识不得撞车");
        assertNotEquals(ActionKeys.forward("key-flow", "inst-c5", "flaky"),
                ActionKeys.forward("key-flow", "inst-c6", "flaky"),
                "判定依据：不同实例的标识不得撞车");
        // 快照中也能直接读到标识，业务方可查询后自行判重
        assertEquals(flakyKeys.get(0), snap.steps().get(0).actionKey());
        assertEquals(ActionKeys.compensation("key-flow", "inst-c5", "flaky"),
                snap.steps().get(0).compensationKey());
    }

    // 6. 兼容范围：功能上线前已落盘的老实例（纯旧格式 JSON）照常恢复 ----------

    @Test
    void legacyPersistedInstanceFromBeforeThisFeatureStillRecovers() throws Exception {
        // 手工落盘一份「老版本」格式的实例与守卫：没有任何新增字段，
        // 状态停留在 RUNNING + 守卫 IN_FLIGHT（等价于老版本进程在确认前崩溃）
        Path instances = tempDir.resolve("instances");
        Path guards = tempDir.resolve("guards");
        java.nio.file.Files.createDirectories(instances);
        java.nio.file.Files.createDirectories(guards);
        java.nio.file.Files.writeString(instances.resolve("legacy-1.json"), """
                {
                  "instanceId": "legacy-1",
                  "type": "legacy-flow",
                  "businessId": "BIZ-LEGACY",
                  "status": "RUNNING",
                  "version": 3,
                  "input": {},
                  "steps": [ {
                    "name": "debit", "index": 0, "status": "RUNNING",
                    "attempts": 0, "compensationAttempts": 0,
                    "lastError": null, "result": null,
                    "updatedMillis": 1700000000000, "records": [ ]
                  } ],
                  "traces": [ ],
                  "createdMillis": 1700000000000,
                  "updatedMillis": 1700000000001
                }
                """);
        java.nio.file.Files.writeString(guards.resolve("legacy-1_debit_FORWARD.json"), """
                {
                  "key": "legacy-1|debit|FORWARD",
                  "state": "IN_FLIGHT",
                  "attempt": 1,
                  "resultJson": null,
                  "error": null,
                  "updatedMillis": 1700000000000
                }
                """);

        BusinessLedger ledger = new BusinessLedger();
        SagaOrchestrator restarted = newOrchestrator();
        restarted.register(new SagaDefinition("legacy-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    log.info("[input] legacy debit replayed with derived key={}", ctx.actionKey());
                    return "DEBITED";
                }, null, 1, 0L, 0L))));

        int recovered = restarted.recoverAll();
        SagaSnapshot after = restarted.snapshot("legacy-1");
        log.info("[assert] legacy instance recovered={} status={} key={}",
                recovered, after.status(), ledger.invocations());

        assertEquals(1, recovered, "判定依据：老格式落盘实例无需迁移即可被识别恢复");
        assertEquals(SagaStatus.COMPLETED, after.status());
        assertEquals(List.of(ActionKeys.forward("legacy-flow", "legacy-1", "debit")),
                ledger.invocations(),
                "判定依据：老实例的稳定标识按同一规则现场派生，与格式升级无关");
        assertTrue(after.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED),
                "判定依据：老实例的在途崩溃同样被识别并解释");
    }
}
