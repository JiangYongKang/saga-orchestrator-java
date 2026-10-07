package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.RecoveryReport;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.ActionKeys;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceType;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 「多个恢复者指向同一份存储并发跑恢复 + 恢复完再手动补跑」专项测试。
 *
 * <p>覆盖真实压测里出现的场景：
 * <ul>
 *   <li>服务启动自动恢复与运维手动恢复撞在一起（多个编排器共享同一持久化目录）；</li>
 *   <li>多个实例各自恢复，败者必须按「该实例已被别人推进」处理，继续恢复后续实例，
 *       不允许因一次冲突整批中断、漏掉其它待恢复实例；</li>
 *   <li>并发恢复以及恢复完再补跑，已生效的正向 / 补偿副作用都不许增多，结论唯一、无矛盾状态。</li>
 * </ul>
 *
 * <p>确定性复现手段：每个编排器安装同一个按实例划分的 {@link CyclicBarrier}，
 * 所有恢复者在某实例「落盘恢复结论之前」对齐，然后一起抢同一个落盘版本——
 * 稳定制造乐观 CAS 冲突，而不是靠概率碰运气。
 */
class ConcurrentRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentRecoveryTest.class);

    @TempDir
    Path tempDir;

    /**
     * 锁文件（{@code locks/}）是跨进程互斥的载体，按设计会一直留在目录里；
     * 这里在用例结束后显式删掉，避免 {@link TempDir} 自动清理因目录非空而报错。
     * 删除能成功本身也验证了所有 OS 文件锁都已释放（通道全部关闭）。
     */
    @AfterEach
    void cleanLockFiles() throws Exception {
        Path locks = tempDir.resolve("locks");
        if (java.nio.file.Files.exists(locks)) {
            try (var paths = java.nio.file.Files.walk(locks)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        java.nio.file.Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new RuntimeException("lock file still held / cannot delete: " + p, e);
                    }
                });
            }
        }
    }

    /** 业务侧去重账本：同一稳定 actionKey 只允许真正生效一次，重放一律短路。 */
    static final class BusinessLedger {
        private final Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
        private final List<String> invocations = new CopyOnWriteArrayList<>();
        private final AtomicInteger sideEffects = new AtomicInteger();

        void applyOnce(String actionKey, String what) {
            invocations.add(actionKey);
            if (appliedKeys.add(actionKey)) {
                int n = sideEffects.incrementAndGet();
                log.info("[business] APPLY {} key={} (first sight -> side effect #{})", what, actionKey, n);
            } else {
                log.info("[business] SUPPRESS {} key={} (replay of same logical action, no new effect)",
                        what, actionKey);
            }
        }

        /**
         * 模拟「崩溃前副作用已经在业务侧落地、但编排器确认未落盘」：
         * 账本里先有这个 key 的真实生效记录（占用判重集合），但不产生新的动作调用。
         * 恢复重放时必须命中 SUPPRESS，不能第二次生效。
         */
        void effectLandedBeforeCrash(String actionKey, String what) {
            if (appliedKeys.add(actionKey)) {
                int n = sideEffects.incrementAndGet();
                log.info("[business] PRE-CRASH landed {} key={} (effect already live before process died, #{})",
                        what, actionKey, n);
            }
        }

        int sideEffects() {
            return sideEffects.get();
        }

        List<String> invocations() {
            return invocations;
        }
    }

    private SagaOrchestrator newRecoverer(BusinessLedger ledger, String type,
                                          java.util.function.Consumer<String> syncPoint,
                                          java.util.function.Function<String, ?> action,
                                          java.util.function.Consumer<String> compensation) {
        SagaOrchestrator orch = new SagaOrchestrator(new FileSagaStore(tempDir));
        orch.setRecoverySyncPoint(syncPoint);
        List<StepDefinition> steps = new ArrayList<>();
        steps.add(new StepDefinition("debit",
                ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    return action.apply(ctx.actionKey());
                },
                compensation == null ? null : ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "release");
                    compensation.accept(ctx.actionKey());
                },
                1, 0L, 0L));
        steps.add(new StepDefinition("credit",
                ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "credit");
                    return action.apply(ctx.actionKey());
                }, null, 1, 0L, 0L));
        orch.register(new SagaDefinition(type, steps));
        return orch;
    }

    /** 手工落盘一个「动作已下发、确认未落盘」的在途崩溃实例（正向或补偿阶段）。 */
    private void stageCrashedInstance(String type, String instanceId, boolean compensating) throws Exception {
        Path instances = tempDir.resolve("instances");
        Path guards = tempDir.resolve("guards");
        java.nio.file.Files.createDirectories(instances);
        java.nio.file.Files.createDirectories(guards);
        String step0Status = compensating ? "COMPENSATING" : "RUNNING";
        String step1Status = compensating ? "FAILED" : "PENDING";
        String instanceStatus = compensating ? "COMPENSATING" : "RUNNING";
        String guardPhase = compensating ? "COMPENSATE" : "FORWARD";
        String guardFileKey = instanceId + "_debit_" + guardPhase;
        java.nio.file.Files.writeString(instances.resolve(instanceId + ".json"), """
                {
                  "instanceId": "%s", "type": "%s", "businessId": "BIZ-%s",
                  "status": "%s", "version": 2, "input": {},
                  "steps": [
                    { "name": "debit", "index": 0, "status": "%s",
                      "attempts": 1, "compensationAttempts": 0,
                      "lastError": null, "result": "DEBITED",
                      "updatedMillis": 1700000000000, "records": [ ] },
                    { "name": "credit", "index": 1, "status": "%s",
                      "attempts": %d, "compensationAttempts": 0,
                      "lastError": %s, "result": null,
                      "updatedMillis": 1700000000001, "records": [ ] }
                  ],
                  "traces": [ ],
                  "createdMillis": 1700000000000, "updatedMillis": 1700000000002
                }
                """.formatted(instanceId, type, instanceId, instanceStatus,
                step0Status, step1Status, compensating ? 1 : 0,
                compensating ? "\"boom\"" : "null"));
        java.nio.file.Files.writeString(guards.resolve(guardFileKey + ".json"), """
                { "key": "%s|debit|%s", "state": "IN_FLIGHT", "attempt": 1,
                  "resultJson": null, "error": null, "updatedMillis": 1700000000000 }
                """.formatted(instanceId, guardPhase));
        if (compensating) {
            // 正向 debit 的成功守卫也必须在盘上（它确认成功过）
            java.nio.file.Files.writeString(guards.resolve(instanceId + "_debit_FORWARD.json"), """
                    { "key": "%s|debit|FORWARD", "state": "DONE", "attempt": 1,
                      "resultJson": "\"DEBITED\"", "error": null, "updatedMillis": 1700000000000 }
                    """.formatted(instanceId));
        }
        log.info("[input] staged crashed instance={} type={} phase={} (in-flight action unconfirmed on disk)",
                instanceId, type, guardPhase);
    }

    /**
     * 让第一波多个恢复者在每个实例的「落盘恢复结论」之前对齐，确定性制造 CAS 竞争。
     *
     * <p>每个实例一扇门：前 {@code parties} 个到达者（= 第一波恢复者）全部到齐才一起放行；
     * 门一旦开过就永久敞开（后续轮次 / 手动补跑不再门控）。这样既能稳定复现
     * 「同一落盘版本上的并发冲突」，又不会让补跑阶段卡在死门上。
     */
    private static final class PerInstanceBarrier {
        private final int parties;
        private final Map<String, java.util.concurrent.atomic.AtomicInteger> arrived = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();

        PerInstanceBarrier(int parties) {
            this.parties = parties;
        }

        void await(String instanceId) {
            java.util.concurrent.atomic.AtomicInteger count =
                    arrived.computeIfAbsent(instanceId, k -> new java.util.concurrent.atomic.AtomicInteger());
            int mine = count.incrementAndGet();
            if (mine > parties) {
                return; // 门已开过（补跑/晚到者），直接放行
            }
            CountDownLatch gate = gates.computeIfAbsent(instanceId, k -> new CountDownLatch(1));
            if (mine == parties) {
                log.info("[sync] all {} recoverers aligned on instance={} -> release the race together",
                        parties, instanceId);
                gate.countDown();
            }
            try {
                if (!gate.await(30, TimeUnit.SECONDS)) {
                    throw new RuntimeException("recovery sync timeout for " + instanceId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("recovery sync interrupted for " + instanceId, e);
            }
        }
    }

    @Test
    void multipleRecoverersSharingOneStoreConvergeWithoutErrorsOrMissedInstances() throws Exception {
        String type = "multi-recoverer-flow";
        int instanceCount = 3;
        int recovererCount = 4;
        List<String> ids = new ArrayList<>();
        BusinessLedger ledger = new BusinessLedger();

        // 输入：三份「动作已下发、确认未落盘」的崩溃遗留状态（共享同一份持久化）
        for (int i = 0; i < instanceCount; i++) {
            String id = "mr-" + i;
            ids.add(id);
            stageCrashedInstance(type, id, false);
            // 等价于：进程被杀前 debit 已经在业务侧真正生效（但确认丢失）。
            // 恢复重放必须被账本挡掉，不许产生第二次 debit 生效。
            ledger.effectLandedBeforeCrash(ActionKeys.forward(type, id, "debit"), "debit");
        }

        PerInstanceBarrier barrier = new PerInstanceBarrier(recovererCount);
        ExecutorService pool = Executors.newFixedThreadPool(recovererCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<RecoveryReport>> futures = new ArrayList<>();

        // 4 个相互独立的恢复者（各自有锁空间，模拟不同实例 / 自动恢复与手动恢复撞车）
        for (int r = 0; r < recovererCount; r++) {
            SagaOrchestrator recoverer = newRecoverer(ledger, type, barrier::await,
                    key -> "OK", null);
            futures.add(pool.submit(() -> {
                start.await();
                return recoverer.recoverAllReport();
            }));
        }
        start.countDown();

        List<RecoveryReport> reports = new ArrayList<>();
        for (Future<RecoveryReport> f : futures) {
            reports.add(f.get(60, TimeUnit.SECONDS)); // 任何恢复调用抛错都会在这里以异常形式炸出来
        }
        pool.shutdown();

        // ---- 判定依据：并发恢复不抛错、恰好每个实例有一个获胜者、败者全部安静认输 ----
        List<String> allRecovered = new ArrayList<>();
        List<String> allDeferred = new ArrayList<>();
        for (int r = 0; r < reports.size(); r++) {
            RecoveryReport rep = reports.get(r);
            allRecovered.addAll(rep.recovered());
            allDeferred.addAll(rep.deferred());
            log.info("[assert] recoverer#{} report: scanned={} recovered={} deferred={} terminal={} failed={}",
                    r, rep.scanned(), rep.recovered(), rep.deferred(),
                    rep.alreadyTerminal(), rep.failed());
            assertTrue(rep.failed().isEmpty(),
                    "判定依据：并发恢复不允许有任何实例以异常失败，更不能把异常抛给调用方");
        }
        assertEquals(ids.size(), allRecovered.size(),
                "判定依据：每个待恢复实例恰好被一个恢复者真正推进");
        assertEquals(new java.util.TreeSet<>(ids), new java.util.TreeSet<>(allRecovered),
                "判定依据：被推进的实例集合完整，没有漏掉");
        int expectedDefeats = instanceCount * (recovererCount - 1);
        assertEquals(expectedDefeats, allDeferred.size(),
                "判定依据：其余 " + (recovererCount - 1) + " 个恢复者对每实例都按「已被别人推进」认输，"
                        + "不重试、不报错、不中断后面的实例");

        // ---- 判定依据：所有实例结论一致、唯一、无矛盾状态 ----
        SagaOrchestrator viewer = newRecoverer(ledger, type, id -> { }, key -> "OK", null);
        for (String id : ids) {
            SagaSnapshot snap = viewer.snapshot(id);
            assertEquals(SagaStatus.COMPLETED, snap.status(),
                    "判定依据：" + id + " 并发恢复后必须收敛到唯一终态 COMPLETED");
            assertFalse(snap.steps().stream().anyMatch(s -> s.inFlight()),
                    "判定依据：" + id + " 终态快照不允许残留在途中间态");
            assertFalse(snap.steps().stream().anyMatch(s -> s.status() == StepStatus.RUNNING),
                    "判定依据：" + id + " 不允许实例终态了某步还显示在途");
            assertTrue(snap.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED),
                    "判定依据：" + id + " 轨迹必须说明在途动作是如何被处置/收敛的");
            long recoveredTraces = snap.traces().stream()
                    .filter(t -> t.getType() == TraceType.RECOVERED).count();
            assertTrue(recoveredTraces >= 1,
                    "判定依据：" + id + " 至少有一个恢复者留下恢复结论，且不存在矛盾终态");
        }

        // ---- 判定依据：副作用数量不随并发恢复增多（正向链路）----
        // 每实例：debit 在崩溃前已生效 1 次（恢复重放被业务账本 SUPPRESS），
        // credit 此前完全未执行，恢复中首次生效 1 次 => 每实例恰好 2 个真实副作用。
        assertEquals(instanceCount * 2, ledger.sideEffects(),
                "判定依据：崩溃前已生效的 debit 不允许第二次生效，未执行的 credit 正常生效一次；"
                        + "并发恢复不增加任何副作用");
        for (String id : ids) {
            String debitKey = ActionKeys.forward(type, id, "debit");
            String creditKey = ActionKeys.forward(type, id, "credit");
            // debit 被重放过（崩溃确认丢失 -> 结果未知必须重放），但只有一次真正生效
            assertTrue(ledger.invocations().stream().anyMatch(debitKey::equals),
                    "判定依据：" + id + " 的在途 debit 必须以同一 actionKey 重放");
            assertTrue(ledger.invocations().stream().anyMatch(creditKey::equals),
                    "判定依据：" + id + " 的未执行 credit 必须在恢复中正常生效");
        }

        // ---- 判定依据：恢复完再手动补跑（再来一轮），全部幂等 no-op、副作用不增多 ----
        int effectsBeforeManual = ledger.sideEffects();
        int invocationsBeforeManual = ledger.invocations().size();
        for (int r = 0; r < recovererCount; r++) {
            SagaOrchestrator again = newRecoverer(ledger, type, id -> { }, key -> "OK", null);
            RecoveryReport rep = again.recoverAllReport();
            log.info("[assert] manual re-run recoverer#{}: recovered={} deferred={} terminal={}",
                    r, rep.recovered(), rep.deferred(), rep.alreadyTerminal());
            assertEquals(0, rep.recovered().size(),
                    "判定依据：恢复完再手动补跑必须全部幂等 no-op");
            assertEquals(ids.size(), rep.alreadyTerminal().size(),
                    "判定依据：补跑时每个实例都应直接识别为终态");
        }
        assertEquals(effectsBeforeManual, ledger.sideEffects(),
                "判定依据：手动补跑后业务副作用数量不许变化");
        // 终态 + 守卫 DONE 短路：补跑不会再真正调用业务动作
        assertEquals(invocationsBeforeManual, ledger.invocations().size(),
                "判定依据：已 DONE 的守卫在补跑时直接短路，业务动作不会再被调用");
    }

    // 补偿链路：在途补偿崩溃 + 多恢复者并发 + 手动补跑 ------------------------

    @Test
    void concurrentRecoveryOfCrashMidCompensationNeitherSkipsNorDoubles() throws Exception {
        String type = "multi-recoverer-comp-flow";
        int instanceCount = 2;
        int recovererCount = 4;
        List<String> ids = new ArrayList<>();
        BusinessLedger ledger = new BusinessLedger();

        // 输入：补偿已经下发（release 在业务侧生效）但确认未落盘时进程被杀，
        // 实例停在 COMPENSATING、debit 步骤停在 COMPENSATING + 守卫 IN_FLIGHT。
        for (int i = 0; i < instanceCount; i++) {
            String id = "mc-" + i;
            ids.add(id);
            stageCrashedInstance(type, id, true);
            // 崩溃前：正向 debit 真正生效过；补偿 release 也真正生效了，只是确认丢失。
            ledger.effectLandedBeforeCrash(ActionKeys.forward(type, id, "debit"), "debit");
            ledger.effectLandedBeforeCrash(ActionKeys.compensation(type, id, "debit"), "release");
        }

        PerInstanceBarrier barrier = new PerInstanceBarrier(recovererCount);
        ExecutorService pool = Executors.newFixedThreadPool(recovererCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<RecoveryReport>> futures = new ArrayList<>();
        for (int r = 0; r < recovererCount; r++) {
            SagaOrchestrator recoverer = newCompensationRecoverer(ledger, type, barrier::await);
            futures.add(pool.submit(() -> {
                start.await();
                return recoverer.recoverAllReport();
            }));
        }
        start.countDown();

        List<RecoveryReport> reports = new ArrayList<>();
        for (Future<RecoveryReport> f : futures) {
            reports.add(f.get(60, TimeUnit.SECONDS));
        }
        pool.shutdown();

        List<String> allRecovered = new ArrayList<>();
        List<String> allDeferred = new ArrayList<>();
        for (int r = 0; r < reports.size(); r++) {
            RecoveryReport rep = reports.get(r);
            allRecovered.addAll(rep.recovered());
            allDeferred.addAll(rep.deferred());
            log.info("[assert] comp recoverer#{}: scanned={} recovered={} deferred={} failed={}",
                    r, rep.scanned(), rep.recovered(), rep.deferred(), rep.failed());
            assertTrue(rep.failed().isEmpty(),
                    "判定依据：补偿并发恢复不允许抛错或整批中断");
        }
        assertEquals(ids.size(), allRecovered.size(),
                "判定依据：每个补偿中的实例恰好被一个恢复者推进到终态");
        assertEquals(new java.util.TreeSet<>(ids), new java.util.TreeSet<>(allRecovered),
                "判定依据：补偿恢复不允许漏掉任何实例");
        assertEquals(instanceCount * (recovererCount - 1), allDeferred.size(),
                "判定依据：败者全部按「已被别人推进」认输并继续处理后续实例");

        SagaOrchestrator viewer = newCompensationRecoverer(ledger, type, id -> { });
        for (String id : ids) {
            SagaSnapshot snap = viewer.snapshot(id);
            assertEquals(SagaStatus.COMPENSATED, snap.status(),
                    "判定依据：" + id + " 在途补偿被并发恢复后必须收敛到唯一终态 COMPENSATED，不得被跳过");
            assertEquals(StepStatus.COMPENSATED, snap.steps().get(0).status(),
                    "判定依据：" + id + " 的在途补偿必须真正完成（重放），不允许残留在途/失败");
            assertEquals(StepStatus.FAILED, snap.steps().get(1).status());
            assertFalse(snap.steps().stream().anyMatch(SagaSnapshot.StepView::inFlight),
                    "判定依据：" + id + " 终态快照不得残留在途标记");
            assertTrue(snap.traces().stream().anyMatch(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED
                            && t.getDetail().contains("COMPENSATE")),
                    "判定依据：" + id + " 轨迹必须解释在途补偿如何收敛");
        }

        // 每实例：debit 正向 1 次 + release 补偿 1 次（崩溃前已落地），
        // 恢复重放补偿时业务 SUPPRESS；credit 失败无正向副作用、不补偿。
        assertEquals(instanceCount * 2, ledger.sideEffects(),
                "判定依据：并发恢复下补偿不允许跳过（补偿要真正完成），也不允许第二次生效；"
                        + "正向副作用同样不重复");
        for (String id : ids) {
            String releaseKey = ActionKeys.compensation(type, id, "debit");
            long releaseCalls = ledger.invocations().stream().filter(releaseKey::equals).count();
            assertTrue(releaseCalls >= 1,
                    "判定依据：" + id + " 的在途补偿必须以同一 actionKey 重放（结果未知，不能假装成功）");
        }

        // 恢复完再手动补跑：COMPENSATED 幂等 no-op，守卫全部 DONE 短路，副作用不增多
        int effectsBeforeManual = ledger.sideEffects();
        int invocationsBeforeManual = ledger.invocations().size();
        for (int r = 0; r < recovererCount; r++) {
            SagaOrchestrator again = newCompensationRecoverer(ledger, type, id -> { });
            RecoveryReport rep = again.recoverAllReport();
            assertEquals(0, rep.recovered().size(),
                    "判定依据：补偿恢复完再手动补跑必须幂等 no-op");
            assertEquals(ids.size(), rep.alreadyTerminal().size());
        }
        assertEquals(effectsBeforeManual, ledger.sideEffects(),
                "判定依据：手动补跑后补偿/正向副作用数量都不许变化");
        assertEquals(invocationsBeforeManual, ledger.invocations().size(),
                "判定依据：已 DONE 的补偿守卫在补跑时直接短路");
    }

    // 跨入口：启动自动恢复进行中，运维对同一实例手动操作（重复 submit / 手动补跑） ----

    @Test
    void automaticRecoveryRacingWithManualSubmitAndReRecoveryConvergesQuietly() throws Exception {
        String type = "entry-race-flow";
        String id = "er-1";
        BusinessLedger ledger = new BusinessLedger();
        stageCrashedInstance(type, id, false);
        ledger.effectLandedBeforeCrash(ActionKeys.forward(type, id, "debit"), "debit");

        // 两个恢复者共享一扇门：在「落盘恢复结论之前」必须对齐，确定性制造同版本 CAS 竞争。
        CountDownLatch bothAligned = new CountDownLatch(2);
        CountDownLatch releaseRace = new CountDownLatch(1);
        java.util.function.Consumer<String> syncPoint = instanceId -> {
            bothAligned.countDown();
            try {
                if (!releaseRace.await(30, TimeUnit.SECONDS)) {
                    throw new RuntimeException("entry-race gate timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        SagaOrchestrator auto = newRecoverer(ledger, type, syncPoint, key -> "OK", null);
        SagaOrchestrator manual = newRecoverer(ledger, type, syncPoint, key -> "OK", null);

        Future<?> autoFuture = poolSubmit(auto::recoverAll);
        Future<RecoveryReport> manualRecoverFuture = poolSubmit(manual::recoverAllReport);
        assertTrue(bothAligned.await(10, TimeUnit.SECONDS),
                "[input] 自动恢复与手动恢复都已进入同一实例，在同一落盘版本上对齐");

        // 起跑同时，运维还并发对同一实例做一次重复提交（第三个入口）
        Future<?> manualSubmitFuture = poolSubmit(() ->
                manual.submit(type, id, "BIZ-" + id, Map.of()));
        releaseRace.countDown();

        autoFuture.get(30, TimeUnit.SECONDS); // 自动恢复绝不抛错
        var submitResult = manualSubmitFuture.get(30, TimeUnit.SECONDS); // 手动提交绝不抛错
        RecoveryReport manualReport = manualRecoverFuture.get(30, TimeUnit.SECONDS);
        log.info("[assert] entry race: auto+manual recovered concurrently, manual submit converged, "
                + "manual recover report={}", manualReport);

        // 自动恢复与手动恢复撞车：终态磁盘轨迹里 RECOVERED 恢复结论只能有一条 ——
        // CAS 败者那条恢复结论写不进盘（被乐观版本挡掉），不会出现两条互相打架的恢复结论。
        long persistedRecoveryClaims = auto.snapshot(id).traces().stream()
                .filter(t -> t.getType() == TraceType.RECOVERED).count();
        assertEquals(1, persistedRecoveryClaims,
                "判定依据：自动恢复与手动恢复撞车时，落盘的恢复结论恰好一条，败者安静认输、"
                        + "不会留下自相矛盾的双恢复轨迹");
        assertTrue(manualReport.failed().isEmpty(),
                "判定依据：手动恢复调用本身绝不抛错、不中断");

        SagaSnapshot finalSnap = auto.snapshot(id);
        assertEquals(SagaStatus.COMPLETED, finalSnap.status(),
                "判定依据：自动恢复与手动操作撞车后，实例收敛到唯一终态");
        assertEquals(SagaStatus.COMPLETED, manual.snapshot(id).status(),
                "判定依据：手动提交调用方拿到的是收敛后的终态快照，而不是冲突异常");
        assertTrue(submitResult != null);
        assertEquals(2, ledger.sideEffects(),
                "判定依据：跨入口并发下，debit 不重复生效、credit 生效一次，副作用不增多");

        // 恢复完两个入口都再手动补跑一次：幂等 no-op
        assertEquals(0, auto.recoverAll());
        assertEquals(0, manual.recoverAll());
        assertEquals(2, ledger.sideEffects(),
                "判定依据：恢复完再补跑，副作用数量保持不变");
        assertEquals(SagaStatus.COMPLETED, auto.snapshot(id).status());
        assertEquals(SagaStatus.COMPLETED, manual.snapshot(id).status(),
                "判定依据：两个入口看到的结论一致，不互相打架");
    }

    private static <T> Future<T> poolSubmit(java.util.concurrent.Callable<T> task) {
        java.util.concurrent.ExecutorService es = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "entry-race-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
        Future<T> f = es.submit(task);
        // 池随任务结束而关闭（守护线程，不影响 JVM 退出）
        es.shutdown();
        return f;
    }

    private static Future<?> poolSubmit(Runnable task) {
        return poolSubmit(Executors.callable(task, null));
    }

    private SagaOrchestrator newCompensationRecoverer(BusinessLedger ledger, String type,
                                                      java.util.function.Consumer<String> syncPoint) {        SagaOrchestrator orch = new SagaOrchestrator(new FileSagaStore(tempDir));
        orch.setRecoverySyncPoint(syncPoint);
        orch.register(new SagaDefinition(type, List.of(
                new StepDefinition("debit",
                        ctx -> "DEBITED",
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "release");
                            log.info("[input] release replay/compensate key={}", ctx.actionKey());
                        },
                        1, 0L, 0L),
                // credit 在崩溃前就失败了（无已确认副作用，不参与补偿）
                new StepDefinition("credit", ctx -> {
                    throw new IllegalStateException("declined");
                }, null, 1, 0L, 0L))));
        return orch;
    }
}
