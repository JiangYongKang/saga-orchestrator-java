package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SubmitResult;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 编排器核心行为测试。每个用例都会打印输入、状态迁移与判定依据（见引擎/守卫日志与本类日志）。
 */
class SagaOrchestratorTest {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestratorTest.class);

    @TempDir
    Path tempDir;

    private SagaOrchestrator newOrchestrator() {
        return new SagaOrchestrator(new FileSagaStore(tempDir));
    }

    private static StepDefinition step(String name, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaAction action) {
        return new StepDefinition(name, action, null, 1, 0L, 0L);
    }

    private static StepDefinition step(String name,
                                       com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaAction action,
                                       com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaCompensation comp) {
        return new StepDefinition(name, action, comp, 1, 0L, 0L);
    }

    // 1. 正常顺序完成 -------------------------------------------------------

    @Test
    void completesAllStepsInOrder() {
        SagaOrchestrator orch = newOrchestrator();
        List<String> order = new ArrayList<>();
        orch.register(new SagaDefinition("order-flow", List.of(
                step("reserve", ctx -> {
                    order.add("reserve");
                    log.info("[input] reserve ctx.businessId={}", ctx.businessId());
                    return "RESERVED";
                }),
                step("pay", ctx -> {
                    order.add("pay");
                    // 前一步结果可被后续读取
                    assertEquals("RESERVED", ctx.stepResults().get("reserve"),
                            "判定依据：后续步骤必须能读到前一步确认结果");
                    return "PAID";
                }),
                step("ship", ctx -> {
                    order.add("ship");
                    return "SHIPPED";
                }))));

        SubmitResult result = orch.submit("order-flow", "inst-1", "B-1", Map.of("amount", 100));
        log.info("[assert] status={} dedup={} order={}", result.snapshot().status(), result.deduplicated(), order);

        assertEquals(SagaStatus.COMPLETED, result.snapshot().status());
        assertFalse(result.deduplicated(), "首次提交不应被判重");
        assertEquals(List.of("reserve", "pay", "ship"), order, "判定依据：必须严格按定义顺序执行");
        SagaSnapshot snap = orch.snapshot("inst-1");
        assertTrue(snap.steps().stream().allMatch(s -> s.status() == StepStatus.SUCCEEDED));
        assertTrue(snap.traces().stream().anyMatch(t -> t.getType().name().equals("COMPLETED")),
                "轨迹中必须包含 COMPLETED 事件");
    }

    // 2. 中途失败 -> 逆序补偿；补偿失败保留中间态并可重试 --------------------

    @Test
    void compensatesInReverseOrderAndRetriesFailedCompensation() {
        SagaOrchestrator orch = newOrchestrator();
        List<String> compensated = CollectionsSync.list();
        AtomicInteger compFailuresLeft = new AtomicInteger(2);

        orch.register(new SagaDefinition("trip-flow", List.of(
                step("bookHotel",
                        ctx -> "H",
                        ctx -> {
                            compensated.add("bookHotel");
                            log.info("[compensate] bookHotel executed");
                        }),
                step("bookFlight",
                        ctx -> "F",
                        ctx -> {
                            // 前两次补偿失败，第三次成功
                            if (compFailuresLeft.getAndDecrement() > 0) {
                                throw new IllegalStateException("flight provider unavailable");
                            }
                            compensated.add("bookFlight");
                            log.info("[compensate] bookFlight succeeded on retry");
                        }),
                step("charge", ctx -> {
                    throw new IllegalStateException("card declined");
                }),
                step("notify", ctx -> "N"))));

        SubmitResult first = orch.submit("trip-flow", "inst-2", "B-2", Map.of());
        log.info("[assert] first submit status={}", first.snapshot().status());
        assertEquals(SagaStatus.COMPENSATION_FAILED, first.snapshot().status(),
                "判定依据：存在失败的补偿时终态必须是 COMPENSATION_FAILED，中间状态不丢失");

        SagaSnapshot s0 = first.snapshot();
        assertEquals(StepStatus.FAILED, s0.steps().get(2).status(),
                "判定依据：重试耗尽的失败步骤为 FAILED");
        assertEquals(StepStatus.SKIPPED, s0.steps().get(3).status(),
                "判定依据：失败步骤之后未执行的步骤必须标记为 SKIPPED");
        assertEquals(StepStatus.COMPENSATION_FAILED, s0.steps().get(1).status(),
                "必须能区分 COMPENSATION_FAILED（待补偿/补偿中分别为 PENDING/COMPENSATING 可查）");
        assertEquals(StepStatus.COMPENSATED, s0.steps().get(0).status(),
                "判定依据：补偿按逆序——bookHotel 先于 bookFlight 完成（bookFlight 失败不阻断后续逆序补偿）");

        // 第二次触发补偿：bookHotel 已 COMPENSATED 必须幂等跳过；bookFlight 重试仍失败
        SagaSnapshot second = orch.triggerCompensation("inst-2");
        assertEquals(SagaStatus.COMPENSATION_FAILED, second.status());
        assertEquals(List.of("bookHotel"), new ArrayList<>(compensated),
                "判定依据：重复补偿不得对已补偿步骤重复生效");

        // 第三次：bookFlight 补偿成功
        SagaSnapshot third = orch.triggerCompensation("inst-2");
        log.info("[assert] third trigger status={}, compensated={}", third.status(), compensated);
        assertEquals(SagaStatus.COMPENSATED, third.status());
        assertEquals(List.of("bookHotel", "bookFlight"), new ArrayList<>(compensated),
                "判定依据：补偿最终按逆序完成且每个补偿只生效一次");
    }

    // 3. 重复提交收敛 -------------------------------------------------------

    @Test
    void duplicateSubmissionConvergesToSameTerminalState() {
        SagaOrchestrator orch = newOrchestrator();
        AtomicInteger runs = new AtomicInteger();
        orch.register(new SagaDefinition("dup-flow", List.of(
                step("only", ctx -> runs.incrementAndGet()))));

        SubmitResult r1 = orch.submit("dup-flow", "inst-3", "B-3", Map.of());
        SubmitResult r2 = orch.submit("dup-flow", "inst-3", "B-3", Map.of());
        log.info("[assert] first dedup={} second dedup={} runs={}", r1.deduplicated(), r2.deduplicated(), runs.get());

        assertFalse(r1.deduplicated());
        assertTrue(r2.deduplicated(), "判定依据：重复提交必须被识别为去重");
        assertEquals(SagaStatus.COMPLETED, r1.snapshot().status());
        assertEquals(SagaStatus.COMPLETED, r2.snapshot().status());
        assertEquals(1, runs.get(), "判定依据：重复提交不得导致步骤重复生效");
        assertEquals(SagaStatus.COMPLETED, orch.snapshot("inst-3").status(),
                "判定依据：重复提交收敛到同一唯一终态");
        assertTrue(r2.snapshot().traces().stream().anyMatch(t -> t.getType().name().equals("SUBMIT_DEDUP")),
                "判定依据：重复提交必须留下可解释的去重轨迹");
    }

    // 4. 并发提交同一实例 -> 唯一终态、动作只生效一次 ------------------------

    @Test
    void concurrentSubmissionsConvergeToSingleTerminalState() throws Exception {
        SagaOrchestrator orch = newOrchestrator();
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch inside = new CountDownLatch(1);
        orch.register(new SagaDefinition("concurrent-flow", List.of(
                step("only", ctx -> {
                    runs.incrementAndGet();
                    try {
                        inside.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }))));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CollectionSync<SagaStatus> statuses = new CollectionSync<>();
        CollectionSync<Boolean> dedups = new CollectionSync<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                SubmitResult r = orch.submit("concurrent-flow", "inst-4", "B-4", Map.of());
                statuses.add(r.snapshot().status());
                dedups.add(r.deduplicated());
            });
        }
        start.countDown();
        // 等待唯一的创建者真正进入动作，再放行动作，避免闭锁顺序造成的空等
        long deadline = System.currentTimeMillis() + 10000;
        while (runs.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(1, runs.get(), "判定依据：只有一个线程能成为创建者并执行动作");
        inside.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        log.info("[assert] statuses={} dedups={} runs={}", statuses, dedups, runs.get());
        assertEquals(1, runs.get(), "判定依据：并发下动作只能生效一次");
        assertEquals(threads, statuses.size());
        assertTrue(statuses.stream().allMatch(s -> s == SagaStatus.COMPLETED),
                "判定依据：所有并发提交必须观察到同一唯一终态 COMPLETED");
        assertEquals(1, dedups.stream().filter(d -> !d).count(),
                "判定依据：恰好一个提交成为创建者，其余全部去重");
    }

    // 5. 步骤重试：确定次数；成功后不重复生效；超时计入尝试 ------------------

    @Test
    void retriesWithDeterministicPolicyAndTimeoutCountsAsAttempt() {
        SagaOrchestrator orch = newOrchestrator();
        AtomicInteger flakyRuns = new AtomicInteger();
        orch.register(new SagaDefinition("retry-flow", List.of(
                new StepDefinition("flaky",
                        ctx -> {
                            int n = flakyRuns.incrementAndGet();
                            log.info("[input] flaky attempt #{}", ctx.attempt());
                            if (n < 3) {
                                throw new IllegalStateException("transient-" + n);
                            }
                            return "OK";
                        },
                        null, 4, 0L, 1L))));

        SubmitResult r = orch.submit("retry-flow", "inst-5", "B-5", Map.of());
        log.info("[assert] status={} runs={}", r.snapshot().status(), flakyRuns.get());
        assertEquals(SagaStatus.COMPLETED, r.snapshot().status());
        assertEquals(3, flakyRuns.get(), "判定依据：失败后重试直到成功，次数由 maxAttempts 确定");
        assertEquals(3, r.snapshot().steps().get(0).attempts());
    }

    @Test
    void timeoutIsRetriedAndEffectivelyCompletesOnce() {
        SagaOrchestrator orch = newOrchestrator();
        AtomicInteger runs = new AtomicInteger();
        orch.register(new SagaDefinition("timeout-flow", List.of(
                new StepDefinition("slow",
                        ctx -> {
                            int attempt = ctx.attempt();
                            runs.incrementAndGet();
                            log.info("[input] slow invoked, engineAttempt={} runCount={}", attempt, runs.get());
                            if (attempt == 1) {
                                Thread.sleep(500); // 超过 80ms 超时
                            }
                            return "DONE";
                        },
                        null, 2, 80L, 0L))));

        SubmitResult r = orch.submit("timeout-flow", "inst-6", "B-6", Map.of());
        log.info("[assert] status={} runs={} traces={}", r.snapshot().status(), runs.get(),
                r.snapshot().traces().stream().map(t -> t.getType().name()).toList());
        assertEquals(SagaStatus.COMPLETED, r.snapshot().status(), "判定依据：超时后按策略重试，第二次成功");
        assertEquals(2, runs.get());
        assertTrue(r.snapshot().steps().get(0).records().stream()
                        .anyMatch(a -> "TIMEOUT".equals(a.getStatus())),
                "判定依据：轨迹中必须能区分 TIMEOUT 与 FAILURE");
    }

    // 6. 重新初始化（重启）后恢复：不重复执行已完成步骤、不跳过补偿 ----------

    @Test
    void recoversAcrossReinitialisationWithoutRepeatingConfirmedWork() {
        // 第一个编排器实例：让 bookFlight 的补偿保持失败 -> COMPENSATION_FAILED 落盘
        SagaOrchestrator first = newOrchestrator();
        List<String> sideEffects = CollectionsSync.list();
        first.register(new SagaDefinition("restart-flow", List.of(
                step("a", ctx -> {
                    sideEffects.add("a-forward");
                    return null;
                }, ctx -> sideEffects.add("a-compensate")),
                step("b", ctx -> {
                    sideEffects.add("b-forward");
                    return null;
                }, ctx -> {
                    throw new IllegalStateException("comp down");
                }),
                step("c", ctx -> {
                    throw new IllegalStateException("boom");
                }))));
        SagaSnapshot before = first.submit("restart-flow", "inst-7", "B-7", Map.of()).snapshot();
        assertEquals(SagaStatus.COMPENSATION_FAILED, before.status());
        int sideEffectCountBeforeRestart = sideEffects.size();

        // 模拟重启：同目录新建编排器，并以"补偿现在能成功"重新注册同名定义
        SagaOrchestrator second = newOrchestrator();
        second.register(new SagaDefinition("restart-flow", List.of(
                step("a", ctx -> {
                    sideEffects.add("a-forward-NEW");
                    return null;
                }, ctx -> sideEffects.add("a-compensate-NEW")),
                step("b", ctx -> {
                    sideEffects.add("b-forward-NEW");
                    return null;
                }, ctx -> sideEffects.add("b-compensate-NEW")),
                step("c", ctx -> {
                    sideEffects.add("c-forward-NEW");
                    return null;
                }))));

        int recovered = second.recoverAll();
        SagaSnapshot after = second.snapshot("inst-7");
        log.info("[assert] recovered={} status={} sideEffects={}", recovered, after.status(), sideEffects);

        assertEquals(1, recovered, "判定依据：必须识别并恢复恰好一个未完成实例");
        assertEquals(SagaStatus.COMPENSATED, after.status());
        assertFalse(sideEffects.contains("a-forward-NEW"),
                "判定依据：恢复不得重新执行已确认成功的正向步骤");
        assertFalse(sideEffects.contains("b-forward-NEW"));
        assertFalse(sideEffects.contains("a-compensate-NEW"),
                "判定依据：已 COMPENSATED 的步骤补偿必须幂等，不重复补偿");
        assertTrue(sideEffects.contains("b-compensate-NEW"),
                "判定依据：恢复不得跳过此前失败（错过）的补偿");
        assertEquals(sideEffectCountBeforeRestart + 1, sideEffects.size(),
                "判定依据：重启后只补做了 b 的一次补偿，无任何重复副作用");
    }

    // 7. 各类拒绝原因可区分 -------------------------------------------------

    @Test
    void rejectsWithDistinguishableReasons() {
        SagaOrchestrator orch = newOrchestrator();
        orch.register(new SagaDefinition("ok-flow", List.of(step("s", ctx -> null))));

        SagaRejectedException unknown = assertThrows(SagaRejectedException.class,
                () -> orch.snapshot("missing"));
        assertEquals(RejectReason.UNKNOWN_INSTANCE, unknown.getReason());
        log.info("[assert] unknown -> {}: {}", unknown.getReason(), unknown.getMessage());

        SagaRejectedException illegalType = assertThrows(SagaRejectedException.class,
                () -> orch.submit("not-registered", UUID.randomUUID().toString(), null, Map.of()));
        assertEquals(RejectReason.ILLEGAL_STATE, illegalType.getReason());

        orch.submit("ok-flow", "inst-8", "B-8", Map.of());
        SagaRejectedException illegalComp = assertThrows(SagaRejectedException.class,
                () -> orch.triggerCompensation("inst-8"));
        assertEquals(RejectReason.ILLEGAL_STATE, illegalComp.getReason(),
                "判定依据：COMPLETED 实例不允许发起补偿，属于非法状态跳转");

        orch.register(new SagaDefinition("dup-type", List.of(step("x", ctx -> null))));
        SagaRejectedException conflictDef = assertThrows(SagaRejectedException.class,
                () -> orch.register(new SagaDefinition("dup-type", List.of(
                        step("x", ctx -> null), step("y", ctx -> null)))));
        assertEquals(RejectReason.CONFLICTING_DEFINITION, conflictDef.getReason());

        SagaRejectedException typeMismatch = assertThrows(SagaRejectedException.class,
                () -> orch.submit("dup-type", "inst-8", "B-8", Map.of()));
        assertEquals(RejectReason.CONFLICTING_DEFINITION, typeMismatch.getReason(),
                "判定依据：同一 instanceId 以不同 type 提交必须被拒绝");
    }

    // 小工具：同步集合，避免并发测试误判 -------------------------------------

    static final class CollectionsSync {
        static <T> List<T> list() {
            return new ArrayList<>() {
                @Override
                public synchronized boolean add(T t) {
                    return super.add(t);
                }
            };
        }
    }

    static final class CollectionSync<T> {
        private final ConcurrentLinkedQueue<T> q = new ConcurrentLinkedQueue<>();

        void add(T t) {
            q.add(t);
        }

        java.util.stream.Stream<T> stream() {
            return q.stream();
        }

        int size() {
            return q.size();
        }

        @Override
        public String toString() {
            return q.toString();
        }
    }
}
