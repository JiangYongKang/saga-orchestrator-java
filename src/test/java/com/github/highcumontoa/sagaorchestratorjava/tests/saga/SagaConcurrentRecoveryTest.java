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

import java.nio.file.Files;
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
 * 多恢复者并发恢复测试：多个编排器（= 多个服务实例 / 启动自动恢复 + 运维手动恢复）
 * 指向同一份落盘存储并发执行 recoverAll，之后再手动补跑。
 *
 * <p>断言：所有恢复调用都正常返回（不因并发冲突报错）、每个实例恰好被一个恢复者推进、
 * 已生效的正向动作与补偿都不第二次生效、没做完的补偿不被跳过、
 * 最终结论唯一且轨迹可解释。日志打印输入、状态迁移与判定依据。
 */
class SagaConcurrentRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(SagaConcurrentRecoveryTest.class);

    /** 所有「服务实例」共享的同一份落盘存储目录。 */
    @TempDir
    Path sharedStoreDir;

    /** 每个恢复者都是独立编排器 + 独立 FileSagaStore 对象，但指向同一目录。 */
    private SagaOrchestrator newRecoverer() {
        return new SagaOrchestrator(new FileSagaStore(sharedStoreDir));
    }

    /** 业务账本：编排器每次（重）调用都会进来，只有首次见到 actionKey 才真正生效。 */
    static final class BusinessLedger {
        private final Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
        private final List<String> invocations = new CopyOnWriteArrayList<>();
        private final AtomicInteger sideEffects = new AtomicInteger();

        void applyOnce(String actionKey, String what) {
            invocations.add(actionKey);
            if (appliedKeys.add(actionKey)) {
                sideEffects.incrementAndGet();
                log.info("[business] APPLY {} key={} (first sight -> side effect #{})",
                        what, actionKey, sideEffects.get());
            } else {
                log.info("[business] SUPPRESS {} key={} (duplicate replay of same logical action)",
                        what, actionKey);
            }
        }

        int sideEffects() {
            return sideEffects.get();
        }

        long invocationsOf(String actionKey) {
            return invocations.stream().filter(actionKey::equals).count();
        }
    }

    /** 「被杀进程」：动作生效后、确认落盘前永久阻塞，随后编排器被丢弃。 */
    private static Thread submitInBackground(SagaOrchestrator orch, String type, String instanceId) {
        Thread t = new Thread(() -> {
            try {
                orch.submit(type, instanceId, "BIZ-" + instanceId, Map.of());
            } catch (RuntimeException e) {
                log.info("[crash-sim] late write from killed process rejected as expected: {}", e.getMessage());
            }
        }, "crash-sim-" + instanceId);
        t.setDaemon(true);
        t.start();
        return t;
    }

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
                log.info("[assert] in-flight visible: instance={} step={} status={} attempt={}",
                        instanceId, stepName, view.status(), view.inFlightAttempt());
                return view;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("step " + stepName + " of " + instanceId + " never became in-flight");
    }

    /** 并发跑多个恢复者，收集每次 recoverAll 的返回或异常。 */
    private static List<Future<Integer>> runRecoverersConcurrently(List<SagaOrchestrator> recoverers,
                                                                   List<Throwable> failures) {
        ExecutorService pool = Executors.newFixedThreadPool(recoverers.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> futures = new ArrayList<>();
        for (SagaOrchestrator r : recoverers) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    return r.recoverAll();
                } catch (Throwable t) {
                    failures.add(t);
                    throw t;
                }
            }));
        }
        start.countDown();
        pool.shutdown();
        try {
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "恢复者线程必须全部结束");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        return futures;
    }

    private static int recoveredSum(List<Future<Integer>> futures) {
        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new AssertionError("recoverAll 不应抛错: " + e.getMessage(), e);
            }
        }).mapToInt(Integer::intValue).sum();
    }

    // 1. 正向链路：多恢复者并发恢复 + 恢复后手动补跑 ------------------------------

    @Test
    void concurrentRecoverersOnForwardChainConvergeWithoutErrors() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);
        List<String> instanceIds = List.of("fwd-1", "fwd-2", "fwd-3");

        // 「被杀进程」：3 个实例都停在 debit 在途（副作用已生效、确认未落盘）
        SagaOrchestrator killed = newRecoverer();
        killed.register(new SagaDefinition("conc-fwd", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    log.info("[input] debit dispatched key={} attempt={}", ctx.actionKey(), ctx.attempt());
                    try {
                        blockKilledProcess.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "DEBITED";
                }, null, 1, 0L, 0L))));
        for (String id : instanceIds) {
            submitInBackground(killed, "conc-fwd", id);
        }
        for (String id : instanceIds) {
            awaitInFlight(killed, id, "debit");
        }
        assertEquals(3, ledger.sideEffects());
        log.info("[crash-sim] 3 instances in-flight, process killed; store dir shared by all recoverers");

        // 三个恢复者（各自独立编排器、同一存储目录）同时恢复：模拟启动自动恢复 +
        // 运维手动触发 + 另一服务实例的恢复撞在一起
        List<SagaOrchestrator> recoverers = List.of(newRecoverer(), newRecoverer(), newRecoverer());
        for (SagaOrchestrator r : recoverers) {
            r.register(new SagaDefinition("conc-fwd", List.of(
                    new StepDefinition("debit", ctx -> {
                        ledger.applyOnce(ctx.actionKey(), "debit");
                        log.info("[input] debit REPLAYED by recoverer key={}", ctx.actionKey());
                        Thread.sleep(20); // 拉宽并发窗口，让「没抢到」的情况真实发生
                        return "DEBITED";
                    }, null, 1, 0L, 0L))));
        }

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Future<Integer>> futures = runRecoverersConcurrently(recoverers, failures);
        int recoveredSum = recoveredSum(futures);

        // 恢复完再手动补跑（每个恢复者都补一次）
        List<Integer> manualReruns = new ArrayList<>();
        for (SagaOrchestrator r : recoverers) {
            manualReruns.add(r.recoverAll());
        }

        SagaOrchestrator querier = recoverers.get(0);
        log.info("[assert] failures={} recoveredSum={} manualReruns={} sideEffects={}",
                failures, recoveredSum, manualReruns, ledger.sideEffects());

        assertTrue(failures.isEmpty(),
                "判定依据：并发恢复不允许有任何一个恢复调用报错中断，实际: " + failures);
        assertEquals(3, recoveredSum,
                "判定依据：每个在途实例恰好被一个恢复者推进，没抢到的按已被推进处理、不重复计数");
        assertTrue(manualReruns.stream().allMatch(n -> n == 0),
                "判定依据：恢复完成后手动补跑是幂等 no-op，实际: " + manualReruns);
        assertEquals(3, ledger.sideEffects(),
                "判定依据：并发恢复 + 手动补跑之后，正向副作用数量不增多");

        for (String id : instanceIds) {
            SagaSnapshot snap = querier.snapshot(id);
            String key = ActionKeys.forward("conc-fwd", id, "debit");
            log.info("[assert] instance={} status={} stepStatus={} inFlight={} invocationsOfKey={}",
                    id, snap.status(), snap.steps().get(0).status(),
                    snap.steps().get(0).inFlight(), ledger.invocationsOf(key));
            assertEquals(SagaStatus.COMPLETED, snap.status(),
                    "判定依据：并发恢复后收敛到唯一终态 COMPLETED");
            assertEquals(StepStatus.SUCCEEDED, snap.steps().get(0).status());
            assertFalse(snap.steps().get(0).inFlight(),
                    "判定依据：终态快照不允许残留矛盾的在途标记");
            assertEquals(2, ledger.invocationsOf(key),
                    "判定依据：该动作恰好被调用两次（崩溃前一次 + 恢复重放一次），并发恢复者不得重复下发");
            assertEquals(1, snap.traces().stream().filter(t -> t.getType() == TraceType.RECOVERED).count(),
                    "判定依据：轨迹中只出现一次恢复结论，多个恢复者不得写出互相打架的恢复记录");
            assertEquals(1, snap.traces().stream()
                    .filter(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED).count());
        }

        blockKilledProcess.countDown();
    }

    // 2. 补偿链路：并发恢复既不跳过补偿也不重复生效 ------------------------------

    @Test
    void concurrentRecoverersOnCompensationChainNeitherSkipNorDouble() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);
        List<String> instanceIds = List.of("comp-1", "comp-2");

        // 「被杀进程」：reserve 成功、charge 失败、reserve 的补偿已下发但未确认（在途崩溃）
        SagaOrchestrator killed = newRecoverer();
        killed.register(new SagaDefinition("conc-comp", List.of(
                new StepDefinition("reserve",
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "reserve");
                            return "RESERVED";
                        },
                        ctx -> {
                            ledger.applyOnce(ctx.actionKey(), "release");
                            log.info("[input] release dispatched key={}", ctx.actionKey());
                            try {
                                blockKilledProcess.await(30, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        1, 0L, 0L),
                new StepDefinition("charge", ctx -> {
                    throw new IllegalStateException("card declined");
                }, null, 1, 0L, 0L))));
        for (String id : instanceIds) {
            submitInBackground(killed, "conc-comp", id);
        }
        for (String id : instanceIds) {
            awaitInFlight(killed, id, "reserve");
        }
        assertEquals(4, ledger.sideEffects(), "reserve x2 + release x2 已生效，确认未落盘");
        log.info("[crash-sim] 2 instances killed mid-compensation");

        List<SagaOrchestrator> recoverers = List.of(newRecoverer(), newRecoverer(), newRecoverer());
        for (SagaOrchestrator r : recoverers) {
            r.register(new SagaDefinition("conc-comp", List.of(
                    new StepDefinition("reserve",
                            ctx -> "RESERVED",
                            ctx -> {
                                ledger.applyOnce(ctx.actionKey(), "release");
                                log.info("[input] release REPLAYED by recoverer key={}", ctx.actionKey());
                                Thread.sleep(20);
                            },
                            1, 0L, 0L),
                    new StepDefinition("charge", ctx -> "X", null, 1, 0L, 0L))));
        }

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Future<Integer>> futures = runRecoverersConcurrently(recoverers, failures);
        int recoveredSum = recoveredSum(futures);
        List<Integer> manualReruns = new ArrayList<>();
        for (SagaOrchestrator r : recoverers) {
            manualReruns.add(r.recoverAll());
        }

        SagaOrchestrator querier = recoverers.get(0);
        log.info("[assert] failures={} recoveredSum={} manualReruns={} sideEffects={}",
                failures, recoveredSum, manualReruns, ledger.sideEffects());

        assertTrue(failures.isEmpty(), "判定依据：并发恢复不允许报错中断，实际: " + failures);
        assertEquals(2, recoveredSum, "判定依据：两个在途补偿实例各恰好被一个恢复者推进");
        assertTrue(manualReruns.stream().allMatch(n -> n == 0));
        assertEquals(4, ledger.sideEffects(),
                "判定依据：reserve x2 + release x2，并发恢复/手动补跑不得让任何副作用多出生效");

        for (String id : instanceIds) {
            SagaSnapshot snap = querier.snapshot(id);
            String reserveKey = ActionKeys.forward("conc-comp", id, "reserve");
            String releaseKey = ActionKeys.compensation("conc-comp", id, "reserve");
            log.info("[assert] instance={} status={} reserve={} charge={} releaseInvocations={}",
                    id, snap.status(), snap.steps().get(0).status(),
                    snap.steps().get(1).status(), ledger.invocationsOf(releaseKey));
            assertEquals(SagaStatus.COMPENSATED, snap.status(),
                    "判定依据：在途补偿必须被继续推进到终态，不得因并发恢复被跳过");
            assertEquals(StepStatus.COMPENSATED, snap.steps().get(0).status());
            assertEquals(StepStatus.FAILED, snap.steps().get(1).status());
            assertEquals(1, ledger.invocationsOf(reserveKey),
                    "判定依据：已确认成功的正向步骤在任何恢复者那里都不重放");
            assertEquals(2, ledger.invocationsOf(releaseKey),
                    "判定依据：补偿恰好重放一次（崩溃前一次 + 恢复一次），并发恢复者不得重复下发");
            assertEquals(1, snap.traces().stream().filter(t -> t.getType() == TraceType.RECOVERED).count());
            assertEquals(1, snap.traces().stream()
                    .filter(t -> t.getType() == TraceType.IN_FLIGHT_RECOVERED).count());
        }

        blockKilledProcess.countDown();
    }

    // 3. 恢复认领（claim）：没抢到的安静收敛；过期认领可被接管 --------------------

    @Test
    void recoveryClaimIsRespectedAndStaleClaimIsTakenOver() throws Exception {
        BusinessLedger ledger = new BusinessLedger();
        CountDownLatch blockKilledProcess = new CountDownLatch(1);

        SagaOrchestrator killed = newRecoverer();
        killed.register(new SagaDefinition("claim-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    try {
                        blockKilledProcess.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "DEBITED";
                }, null, 1, 0L, 0L))));
        submitInBackground(killed, "claim-flow", "claim-1");
        awaitInFlight(killed, "claim-1", "debit");

        // 另一个恢复者正持有该实例的有效认领：本次恢复应安静跳过、不报错、不推进
        Path claims = sharedStoreDir.resolve("claims");
        Files.createDirectories(claims);
        long futureLease = System.currentTimeMillis() + 60_000;
        Files.writeString(claims.resolve("claim-1.json"), """
                { "instanceId": "claim-1", "ownerId": "other-recoverer",
                  "acquiredMillis": %d, "leaseUntilMillis": %d }
                """.formatted(futureLease - 60_000, futureLease));

        SagaOrchestrator recoverer = newRecoverer();
        recoverer.register(new SagaDefinition("claim-flow", List.of(
                new StepDefinition("debit", ctx -> {
                    ledger.applyOnce(ctx.actionKey(), "debit");
                    return "DEBITED";
                }, null, 1, 0L, 0L))));

        int skipped = recoverer.recoverAll();
        log.info("[assert] valid claim held by other recoverer -> resumed={} status={}",
                skipped, recoverer.snapshot("claim-1").status());
        assertEquals(0, skipped,
                "判定依据：认领被其它恢复者持有时，本恢复者按「已被别人推进」安静收敛，不报错不推进");
        assertEquals(SagaStatus.RUNNING, recoverer.snapshot("claim-1").status(),
                "判定依据：被跳过的实例保持原样，等持有者优先推进；本次不漏记也不误推进");

        // 认领过期（持有者崩溃/失联）：必须能接管并恢复，不漏实例
        long expiredLease = System.currentTimeMillis() - 1000;
        Files.writeString(claims.resolve("claim-1.json"), """
                { "instanceId": "claim-1", "ownerId": "dead-recoverer",
                  "acquiredMillis": %d, "leaseUntilMillis": %d }
                """.formatted(expiredLease - 60_000, expiredLease));

        int takenOver = recoverer.recoverAll();
        SagaSnapshot after = recoverer.snapshot("claim-1");
        log.info("[assert] stale claim -> resumed={} status={} sideEffects={}",
                takenOver, after.status(), ledger.sideEffects());
        assertEquals(1, takenOver, "判定依据：过期认领必须被接管，实例不得被漏掉");
        assertEquals(SagaStatus.COMPLETED, after.status());
        assertEquals(1, ledger.sideEffects(), "判定依据：接管恢复后副作用仍然只生效一次");

        // 恢复完成后认领已释放，手动补跑幂等
        assertEquals(0, recoverer.recoverAll());
        assertEquals(1, ledger.sideEffects());

        blockKilledProcess.countDown();
    }
}
