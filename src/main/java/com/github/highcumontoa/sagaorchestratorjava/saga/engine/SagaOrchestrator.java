package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.ActionContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.ActionKeys;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.AttemptRecord;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepExecution;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SubmitResult;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceEvent;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceType;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.IdempotentActionGuard;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.GuardRecord;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.SagaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分布式事务编排器（本地存储版）。
 *
 * <p>核心保证：
 * <ul>
 *   <li><b>顺序正向 + 逆序补偿</b>：按定义顺序执行；某步重试耗尽后，已成功步骤逆序补偿。</li>
 *   <li><b>幂等不重复生效</b>：动作经 {@link IdempotentActionGuard} 守卫，DONE 即短路；
 *       超时使用 attempt fencing，迟到的旧回调被丢弃。</li>
 *   <li><b>并发收敛</b>：每实例一把执行锁，重复/并发提交同一 instanceId 串行收敛到唯一终态。</li>
 *   <li><b>重启恢复</b>：所有状态落盘；{@link #recoverAll()} 继续未完成实例，
 *       已确认完成的步骤不重复执行，未完成的补偿不跳过。</li>
 *   <li><b>可解释</b>：每次输入、状态迁移、判定依据都写轨迹并打日志。</li>
 * </ul>
 */
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private static final String PHASE_FORWARD = "FORWARD";
    private static final String PHASE_COMPENSATE = "COMPENSATE";

    /** 跨恢复者 / 跨入口乐观冲突后，最多基于最新落盘状态重读重驱多少次后才放弃。 */
    private static final int MAX_CAS_ATTEMPTS = 20;

    private final SagaStore store;
    private final IdempotentActionGuard guard;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, SagaDefinition> definitions = new ConcurrentHashMap<>();
    /** recoverAll 时发现「有未完成实例但定义尚未注册」的类型；定义补齐后自动补恢复，保证不漏。 */
    private final java.util.Set<String> typesAwaitingDefinition = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, ReentrantLock> executionLocks = new ConcurrentHashMap<>();
    /**
     * 仅供确定性测试使用的恢复同步点：每个待恢复实例在写入「恢复结论」之前回调一次
     * （参数为 instanceId），可借此让多个恢复者在同一状态版本上对齐，稳定复现 CAS 竞争。
     * 生产环境不设置（null），行为完全不变。
     */
    private volatile java.util.function.Consumer<String> recoverySyncPoint;
    private final ExecutorService actionExecutors = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "saga-action-" + System.nanoTime());
        t.setDaemon(true);
        return t;
    });
    private final AtomicLong fenceCounter = new AtomicLong();

    public SagaOrchestrator(SagaStore store) {
        this.store = store;
        this.guard = new IdempotentActionGuard(store);
        log.info("[orchestrator] created with store {}", store.getClass().getSimpleName());
    }

    /**
     * 安装「恢复竞争点」回调：每个待恢复实例在落盘恢复结论前被回调一次。
     * 仅用于并发恢复测试中把多个恢复者对齐到同一落盘版本，确定性复现冲突；
     * 生产代码不要调用。传 null 移除。
     */
    public void setRecoverySyncPoint(java.util.function.Consumer<String> syncPoint) {
        this.recoverySyncPoint = syncPoint;
    }

    // ---------------------------------------------------------------------
    // 定义注册
    // ---------------------------------------------------------------------

    /** 注册编排模板；同名模板步骤结构不一致时以 CONFLICTING_DEFINITION 明确拒绝。 */
    public void register(SagaDefinition definition) {
        SagaDefinition existing = definitions.get(definition.type());
        if (existing != null && !signature(existing).equals(signature(definition))) {
            throw new SagaRejectedException(RejectReason.CONFLICTING_DEFINITION,
                    "saga type '" + definition.type() + "' already registered with steps "
                            + signature(existing) + ", cannot re-register with " + signature(definition));
        }
        definitions.put(definition.type(), definition);
        log.info("[orchestrator] registered saga type='{}' steps={}", definition.type(), signature(definition));
        if (typesAwaitingDefinition.remove(definition.type())) {
            log.info("[orchestrator] definition for type='{}' arrived after recovery scan; "
                    + "running deferred recovery so no unfinished instance is missed", definition.type());
            recoverType(definition.type());
        }
    }

    private static List<String> signature(SagaDefinition d) {
        return d.steps().stream().map(StepDefinition::name).toList();
    }

    // ---------------------------------------------------------------------
    // 提交（幂等 + 并发收敛）
    // ---------------------------------------------------------------------

    public SubmitResult submit(String type, String instanceId, String businessId, Map<String, Object> input) {
        SagaDefinition def = definitions.get(type);
        if (def == null) {
            throw new SagaRejectedException(RejectReason.ILLEGAL_STATE,
                    "unknown saga type '" + type + "', register a definition first");
        }
        Map<String, Object> safeInput = input == null ? Map.of() : new LinkedHashMap<>(input);
        log.info("[submit] input: type={} instanceId={} businessId={} input={}",
                type, instanceId, businessId, safeInput);

        SagaInstance candidate = newInstance(type, instanceId, businessId, safeInput, def);
        var existing = store.createIfAbsent(candidate);
        boolean duplicated = existing.isPresent();
        if (duplicated && !type.equals(existing.get().getType())) {
            throw new SagaRejectedException(RejectReason.CONFLICTING_DEFINITION,
                    "instance " + instanceId + " already exists with type '" + existing.get().getType()
                            + "' but submitted as '" + type + "'");
        }

        ReentrantLock lock = executionLocks.computeIfAbsent(instanceId, k -> new ReentrantLock());
        lock.lock();
        try {
            // 在实例锁内基于最新落盘状态驱动；若与另一个恢复者 / 提交者（其它 JVM）撞车导致
            // 乐观版本冲突，重读最新状态、按幂等状态机重新驱动，调用方始终拿到收敛后的快照而非报错。
            boolean dedupTraceWritten = false;
            SagaSnapshot snapshot = null;
            for (int attempt = 1; attempt <= MAX_CAS_ATTEMPTS; attempt++) {
                SagaInstance fresh = require(instanceId);
                if (duplicated && !dedupTraceWritten) {
                    try {
                        trace(fresh, TraceType.SUBMIT_DEDUP, null,
                                "duplicate/concurrent submit converged to existing instance in status "
                                        + fresh.getStatus());
                        persist(fresh);
                        dedupTraceWritten = true;
                    } catch (SagaRejectedException conflict) {
                        if (conflict.getReason() != RejectReason.VERSION_CONFLICT) {
                            throw conflict;
                        }
                        log.info("[submit] instance={} dedup-trace write lost optimistic race at attempt={} "
                                + "(advanced concurrently) -> re-read and retry", instanceId, attempt);
                        continue; // 本轮未成功落盘任何内容，基于最新状态整轮重来
                    }
                }
                try {
                    resume(fresh, def);
                    snapshot = toSnapshot(require(instanceId));
                    break;
                } catch (SagaRejectedException conflict) {
                    if (conflict.getReason() != RejectReason.VERSION_CONFLICT) {
                        throw conflict;
                    }
                    log.info("[submit] instance={} lost optimistic race while resuming at attempt={} "
                            + "(advanced by a concurrent recoverer) -> re-read latest state and resume",
                            instanceId, attempt);
                    if (attempt == MAX_CAS_ATTEMPTS) {
                        throw conflict;
                    }
                }
            }
            if (snapshot == null) {
                // 理论上不可达：上面的冲突在达到上限时已直接抛出；防御性兜底，绝不返回 null
                throw new SagaRejectedException(RejectReason.VERSION_CONFLICT,
                        "submit for instance " + instanceId
                                + " kept losing optimistic races against concurrent recoverers; "
                                + "call recoverAll or retry submit");
            }
            log.info("[submit] decision: instanceId={} deduplicated={} finalStatus={}",
                    instanceId, duplicated, snapshot.status());
            return new SubmitResult(snapshot, duplicated);
        } finally {
            lock.unlock();
        }
    }

    private SagaInstance newInstance(String type, String instanceId, String businessId,
                                     Map<String, Object> input, SagaDefinition def) {
        long now = System.currentTimeMillis();
        SagaInstance inst = new SagaInstance();
        inst.setInstanceId(instanceId);
        inst.setType(type);
        inst.setBusinessId(businessId);
        inst.setInput(input);
        inst.setCreatedMillis(now);
        inst.setUpdatedMillis(now);
        List<StepExecution> steps = new ArrayList<>();
        for (int i = 0; i < def.steps().size(); i++) {
            StepExecution se = new StepExecution();
            se.setName(def.steps().get(i).name());
            se.setIndex(i);
            se.setUpdatedMillis(now);
            steps.add(se);
        }
        inst.setSteps(steps);
        trace(inst, TraceType.SUBMITTED, null, "instance accepted with input " + input);
        return inst;
    }

    // ---------------------------------------------------------------------
    // 恢复 / 状态机驱动
    // ---------------------------------------------------------------------

    /**
     * 重启或重新初始化后，继续所有未完成（含补偿未完成）的实例。
     *
     * <p>幂等且<b>可并发调用</b>：自动恢复、运维手动恢复、多个恢复者共享同一份存储同时跑都安全。
     * 返回值为本调用真正推进的实例数；冲突败北（该实例已被别的恢复者推进）的不计入、也不抛错，
     * 任何单个实例的异常都不会中断对其余实例的恢复。
     */
    public int recoverAll() {
        return recoverAllReport().recovered().size();
    }

    /**
     * 与 {@link #recoverAll()} 相同的恢复动作，但返回可解释的分类报告：
     * 哪些由本调用推进、哪些因并发冲突按「已被别人推进」认输跳过、
     * 哪些本来就是终态 / 等待定义 / 发生了单实例异常。
     */
    public RecoveryReport recoverAllReport() {
        List<SagaInstance> all = store.findAll();
        List<String> recovered = new ArrayList<>();
        List<String> deferred = new ArrayList<>();
        List<String> alreadyTerminal = new ArrayList<>();
        List<String> awaitingDefinition = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        for (SagaInstance inst : all) {
            if (isTerminal(inst.getStatus())) {
                alreadyTerminal.add(inst.getInstanceId());
                continue;
            }
            try {
                switch (recoverInstance(inst)) {
                    case RECOVERED -> recovered.add(inst.getInstanceId());
                    case DEFERRED -> deferred.add(inst.getInstanceId());
                    case AWAITING_DEFINITION -> awaitingDefinition.add(inst.getInstanceId());
                }
            } catch (RuntimeException perInstanceFailure) {
                // 单个实例的任何异常都只隔离在它自己身上：记录后继续处理后面的待恢复实例，绝不整批中断
                failed.add(inst.getInstanceId());
                log.error("[recover] instance={} failed in this recovery pass and will be retried "
                        + "on the next recoverAll(); remaining instances continue unaffected: {}",
                        inst.getInstanceId(), perInstanceFailure.toString());
            }
        }
        RecoveryReport report = new RecoveryReport(List.copyOf(recovered), List.copyOf(deferred),
                List.copyOf(alreadyTerminal), List.copyOf(awaitingDefinition),
                List.copyOf(failed), all.size());
        log.info("[recover] scanned {} instances: recovered={}, deferredToOtherRecoverer={}, "
                        + "alreadyTerminal={}, awaitingDefinition={}, failed={}",
                all.size(), recovered, deferred, alreadyTerminal, awaitingDefinition, failed);
        return report;
    }

    /** 只恢复某个类型的未完成实例（定义晚于恢复扫描注册时的补恢复）。 */
    private void recoverType(String type) {
        for (SagaInstance inst : store.findAll()) {
            if (type.equals(inst.getType()) && !isTerminal(inst.getStatus())) {
                try {
                    recoverInstance(inst);
                } catch (RuntimeException perInstanceFailure) {
                    // 补恢复同样不得因为单实例冲突/异常中断同类型其余实例
                    log.error("[recover] deferred recovery for type={} instance={} failed; "
                            + "will retry on next recoverAll(): {}",
                            type, inst.getInstanceId(), perInstanceFailure.toString());
                }
            }
        }
    }

    /** 单个实例恢复的分类结论。 */
    private enum RecoveryOutcome {
        /** 本调用写入恢复结论并把实例推进到了终态。 */
        RECOVERED,
        /** 该实例在本调用处理期间已被并发的其它恢复者推进，本调用安静认输跳过。 */
        DEFERRED,
        /** 本进程缺少对应类型定义，已登记待补恢复。 */
        AWAITING_DEFINITION
    }

    /**
     * 恢复单个实例。
     *
     * <p>在实例锁内重读状态：已被并发恢复推进到终态的实例直接收敛，不计数、不追加矛盾轨迹；
     * 若落盘恢复结论（或恢复过程中的任何一次状态落盘）被另一个恢复者抢先——
     * 即乐观版本 CAS 失败——说明该实例正由 / 已由别人推进，本调用按 {@link RecoveryOutcome#DEFERRED}
     * 安静认输，绝不重试、绝不报错，交由 {@link #recoverAllReport()} 继续处理后面的实例。
     */
    private RecoveryOutcome recoverInstance(SagaInstance scanned) {
        SagaStatus status = scanned.getStatus();
        boolean unfinished = status == SagaStatus.PENDING || status == SagaStatus.RUNNING
                || status == SagaStatus.COMPENSATING
                || status == SagaStatus.COMPENSATION_FAILED;
        if (!unfinished) {
            return RecoveryOutcome.DEFERRED;
        }
        ReentrantLock lock = executionLocks.computeIfAbsent(scanned.getInstanceId(), k -> new ReentrantLock());
        lock.lock();
        try {
            SagaInstance fresh = require(scanned.getInstanceId());
            if (isTerminal(fresh.getStatus())) {
                log.info("[recover] instance={} already converged to terminal {} by a concurrent recovery "
                        + "-> idempotent no-op (treated as advanced by another recoverer)",
                        fresh.getInstanceId(), fresh.getStatus());
                return RecoveryOutcome.DEFERRED;
            }
            SagaDefinition resolved = definitions.get(fresh.getType());
            if (resolved == null) {
                typesAwaitingDefinition.add(fresh.getType());
                log.warn("[recover] instance={} type='{}' has no registered definition in this process; "
                        + "deferring recovery — it will resume automatically once the definition is registered "
                        + "(or call recoverAll again after registering)",
                        fresh.getInstanceId(), fresh.getType());
                return RecoveryOutcome.AWAITING_DEFINITION;
            }
            // 确定性并发测试的竞争对齐点（生产环境为空）
            java.util.function.Consumer<String> sync = recoverySyncPoint;
            if (sync != null) {
                sync.accept(fresh.getInstanceId());
            }
            try {
                trace(fresh, TraceType.RECOVERED, null,
                        "resume after (re)initialisation from status " + fresh.getStatus()
                                + "; already-confirmed steps will not be re-executed");
                persist(fresh);
                log.info("[recover] resuming instance={} from status={}", fresh.getInstanceId(), fresh.getStatus());
                resume(fresh, resolved);
                return RecoveryOutcome.RECOVERED;
            } catch (SagaRejectedException versionConflict) {
                if (versionConflict.getReason() != RejectReason.VERSION_CONFLICT) {
                    throw versionConflict;
                }
                // 另一个恢复者已经抢先落盘推进：本调用安静认输，后续实例继续处理
                SagaSnapshot winnerView = peekSnapshotOrNull(scanned.getInstanceId());
                log.info("[recover] instance={} was advanced by a concurrent recoverer while this call was "
                                + "resuming it (CAS lost, winnerStatus={}) -> defer quietly, no retry here, "
                                + "remaining instances keep being recovered",
                        scanned.getInstanceId(),
                        winnerView == null ? "unknown" : winnerView.status());
                return RecoveryOutcome.DEFERRED;
            }
        } finally {
            lock.unlock();
        }
    }

    private SagaSnapshot peekSnapshotOrNull(String instanceId) {
        try {
            return toSnapshot(require(instanceId));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static boolean isTerminal(SagaStatus status) {
        return status == SagaStatus.COMPLETED || status == SagaStatus.COMPENSATED
                || status == SagaStatus.FAILED;
    }

    /** 根据当前状态选择推进路径；终态直接收敛返回。 */
    private void resume(SagaInstance inst, SagaDefinition def) {
        if (def == null) {
            throw new SagaRejectedException(RejectReason.ILLEGAL_STATE,
                    "no definition registered for type '" + inst.getType()
                            + "'; register it before resuming/recovering");
        }
        explainOrphanedInFlight(inst);
        switch (inst.getStatus()) {
            case PENDING, RUNNING -> runForward(inst, def);
            case COMPENSATING, COMPENSATION_FAILED -> runCompensation(inst, def);
            case COMPLETED, COMPENSATED, FAILED -> log.info(
                    "[resume] instance={} already in terminal status {} -> converge",
                    inst.getInstanceId(), inst.getStatus());
        }
    }

    /**
     * 在进入状态机之前识别「动作已下发、结果未确认」的在途步骤（崩溃/被杀遗留）。
     *
     * <p>resume 只在持有实例锁、且本进程没有任何线程正在执行该实例时被调用，
     * 因此这里看到的 RUNNING/COMPENSATING + 守卫 IN_FLIGHT 一定是上一次进程生命
     * 里没落盘确认的孤儿尝试。对每个这样的步骤写一条 {@link TraceType#IN_FLIGHT_RECOVERED}
     * 轨迹，说明处置方式：以同一个稳定 actionKey 重放该逻辑动作，由业务侧按键判重，
     * 编排器既不重复确认、也不跳过任何补偿。
     */
    private void explainOrphanedInFlight(SagaInstance inst) {
        boolean traced = false;
        for (StepExecution se : inst.getSteps()) {
            String phase = switch (se.getStatus()) {
                case RUNNING -> ActionKeys.PHASE_FORWARD;
                case COMPENSATING -> ActionKeys.PHASE_COMPENSATE;
                default -> null;
            };
            if (phase == null) {
                continue;
            }
            String gk = IdempotentActionGuard.key(inst.getInstanceId(), se.getName(), phase);
            Optional<GuardRecord> guard = store.findGuard(gk);
            if (guard.isEmpty() || !IdempotentActionGuard.IN_FLIGHT.equals(guard.get().getState())) {
                continue;
            }
            GuardRecord g = guard.get();
            String actionKey = ActionKeys.of(inst.getType(), inst.getInstanceId(), se.getName(), phase);
            trace(inst, TraceType.IN_FLIGHT_RECOVERED, se.getName(),
                    "orphaned in-flight " + phase + " attempt " + g.getAttempt()
                            + " detected at resume: action was dispatched but its outcome was never confirmed "
                            + "(process died before confirmation was persisted). Decision: replay the SAME "
                            + "logical action with stable actionKey=" + actionKey
                            + " — the business side deduplicates by this key, so an effect that already landed "
                            + "must not be applied twice; nothing is skipped");
            traced = true;
            log.info("[recover] instance={} step={} phase={} orphaned in-flight attempt={} actionKey={} "
                            + "-> replay with same key, business dedupes by key",
                    inst.getInstanceId(), se.getName(), phase, g.getAttempt(), actionKey);
        }
        if (traced) {
            persist(inst);
        }
    }

    // ---------------------------------------------------------------------
    // 正向执行
    // ---------------------------------------------------------------------

    private void runForward(SagaInstance inst, SagaDefinition def) {
        if (inst.getStatus() == com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.PENDING) {
            transition(inst, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.RUNNING,
                    "start forward execution");
        }
        for (int i = 0; i < def.steps().size(); i++) {
            StepDefinition step = def.steps().get(i);
            StepExecution se = inst.getSteps().get(i);
            if (se.getStatus() == com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SUCCEEDED) {
                log.info("[forward] instance={} step={} already SUCCEEDED -> skip (no duplicate execution)",
                        inst.getInstanceId(), step.name());
                continue;
            }
            if (se.getStatus() == com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SKIPPED) {
                continue;
            }
            boolean ok = executeStepWithRetries(inst, def, step, se, PHASE_FORWARD);
            if (!ok) {
                for (int j = i + 1; j < def.steps().size(); j++) {
                    StepExecution later = inst.getSteps().get(j);
                    if (later.getStatus() == com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.PENDING) {
                        later.setStatus(com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SKIPPED);
                        later.setUpdatedMillis(System.currentTimeMillis());
                        trace(inst, TraceType.STEP_SKIPPED, later.getName(),
                                "skipped because earlier step '" + step.name() + "' failed");
                    }
                }
                persist(inst);
                runCompensation(inst, def);
                return;
            }
        }
        transition(inst, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.COMPLETED,
                "all " + def.steps().size() + " steps succeeded");
    }

    // ---------------------------------------------------------------------
    // 逆序补偿
    // ---------------------------------------------------------------------

    public SagaSnapshot triggerCompensation(String instanceId) {
        ReentrantLock lock = executionLocks.computeIfAbsent(instanceId, k -> new ReentrantLock());
        lock.lock();
        try {
            log.info("[compensate] manual trigger instance={}", instanceId);
            SagaSnapshot snapshot = null;
            for (int attempt = 1; attempt <= MAX_CAS_ATTEMPTS; attempt++) {
                SagaInstance inst = require(instanceId);
                SagaDefinition def = definitions.get(inst.getType());
                SagaStatus current = inst.getStatus();
                log.info("[compensate] resume instance={} currentStatus={}", instanceId, current);
                try {
                    switch (current) {
                        case COMPENSATION_FAILED, COMPENSATING -> runCompensation(inst, def);
                        case COMPENSATED -> log.info(
                                "[compensate] instance={} already COMPENSATED -> idempotent no-op",
                                instanceId);
                        default -> throw new SagaRejectedException(RejectReason.ILLEGAL_STATE,
                                "cannot trigger compensation for instance " + instanceId
                                        + " in status " + current);
                    }
                    snapshot = toSnapshot(require(instanceId));
                    break;
                } catch (SagaRejectedException conflict) {
                    if (conflict.getReason() != RejectReason.VERSION_CONFLICT) {
                        throw conflict;
                    }
                    log.info("[compensate] instance={} lost optimistic race at attempt={} "
                            + "(advanced by a concurrent recoverer) -> re-read latest state and converge",
                            instanceId, attempt);
                    if (attempt == MAX_CAS_ATTEMPTS) {
                        throw conflict;
                    }
                }
            }
            if (snapshot == null) {
                throw new SagaRejectedException(RejectReason.VERSION_CONFLICT,
                        "triggerCompensation for instance " + instanceId
                                + " kept losing optimistic races against concurrent recoverers; "
                                + "call recoverAll or retry");
            }
            return snapshot;
        } finally {
            lock.unlock();
        }
    }

    private void runCompensation(SagaInstance inst, SagaDefinition def) {
        if (inst.getStatus() != com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.COMPENSATING) {
            transition(inst, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.COMPENSATING,
                    "begin reverse compensation");
        }
        boolean anyFailed = false;
        for (int i = def.steps().size() - 1; i >= 0; i--) {
            StepDefinition step = def.steps().get(i);
            StepExecution se = inst.getSteps().get(i);
            switch (se.getStatus()) {
                case COMPENSATED -> log.info("[compensate] step={} already COMPENSATED -> idempotent skip",
                        step.name());
                case PENDING, SKIPPED -> {
                    se.setStatus(com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SKIPPED);
                    se.setUpdatedMillis(System.currentTimeMillis());
                    trace(inst, TraceType.STEP_COMPENSATED, step.name(),
                            "never confirmed success, nothing to compensate -> skipped");
                    persist(inst);
                }
                case FAILED -> trace(inst, TraceType.STEP_COMPENSATED, step.name(),
                        "failed step has no confirmed effect -> not compensated");
                default -> {
                    if (step.compensation() == null) {
                        se.setStatus(com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.COMPENSATED);
                        se.setUpdatedMillis(System.currentTimeMillis());
                        trace(inst, TraceType.STEP_COMPENSATED, step.name(),
                                "no compensation registered, treated as compensated");
                        persist(inst);
                        break;
                    }
                    boolean ok = executeStepWithRetries(inst, def, step, se, PHASE_COMPENSATE);
                    if (!ok) {
                        anyFailed = true;
                    }
                }
            }
        }
        if (anyFailed) {
            transition(inst, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.COMPENSATION_FAILED,
                    "at least one compensation failed; intermediate state retained, re-trigger compensation to retry");
        } else {
            transition(inst, com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus.COMPENSATED,
                    "reverse compensation finished for all confirmed steps");
        }
    }

    // ---------------------------------------------------------------------
    // 单步执行（正向/补偿共用）：守卫 + 超时 + 确定重试
    // ---------------------------------------------------------------------

    private boolean executeStepWithRetries(SagaInstance inst, SagaDefinition def, StepDefinition step,
                                           StepExecution se, String phase) {
        boolean compensate = PHASE_COMPENSATE.equals(phase);
        int maxAttempts = step.maxAttempts();
        while (true) {
            int attempt = (compensate ? se.getCompensationAttempts() : se.getAttempts()) + 1;
            String gk = IdempotentActionGuard.key(inst.getInstanceId(), step.name(), phase);
            IdempotentActionGuard.BeginResult br = guard.begin(gk, attempt);
            if (br.outcome() == IdempotentActionGuard.Outcome.ALREADY_DONE) {
                Object reused = deserialize(br.resultJson());
                log.info("[{}] instance={} step={} guard DONE(attempt={}) -> reuse result, skip action",
                        phase, inst.getInstanceId(), step.name(), br.previousAttempt());
                finishStepSuccess(inst, se, compensate, reused, attempt,
                        "idempotent reuse: side effect already confirmed at attempt " + br.previousAttempt(),
                        compensate ? TraceType.STEP_COMPENSATED : TraceType.STEP_SUCCEEDED);
                return true;
            }

            se.setStatus(compensate
                    ? com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.COMPENSATING
                    : com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.RUNNING);
            trace(inst, compensate
                            ? (attempt == 1 ? TraceType.STEP_COMPENSATE_START : TraceType.STEP_COMPENSATE_RETRY)
                            : (attempt == 1 ? TraceType.STEP_START : TraceType.STEP_RETRY),
                    step.name(), "attempt " + attempt + "/" + maxAttempts
                            + " (guard previous=" + br.previousState() + ") timeout=" + step.timeoutMillis() + "ms");
            persist(inst);

            ActionContext ctx = buildContext(inst, def, step, attempt, phase);
            AttemptRecord rec = new AttemptRecord();
            rec.setAttempt(attempt);
            rec.setPhase(phase);
            rec.setStartMillis(System.currentTimeMillis());

            Object result = null;
            String error = null;
            boolean timedOut = false;

            Future<?> future = compensate
                    ? actionExecutors.submit(() -> {
                        step.compensation().compensate(ctx);
                        return null;
                    })
                    : actionExecutors.submit(() -> step.action().execute(ctx));
            try {
                result = step.timeoutMillis() > 0
                        ? future.get(step.timeoutMillis(), TimeUnit.MILLISECONDS)
                        : future.get();
                guard.complete(gk, attempt, result == null ? null : serialize(result));
                rec.setStatus("SUCCESS");
            } catch (TimeoutException e) {
                timedOut = true;
                future.cancel(true);
                error = "timeout after " + step.timeoutMillis() + "ms";
                rec.setStatus("TIMEOUT");
                // 不调用 guard.fail：保留 IN_FLIGHT + attempt fencing，迟到结果将被丢弃
            } catch (ExecutionException e) {
                error = rootMessage(e);
                guard.fail(gk, attempt, error);
                rec.setStatus("FAILURE");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                error = "interrupted";
                guard.fail(gk, attempt, error);
                rec.setStatus("FAILURE");
            } finally {
                rec.setEndMillis(System.currentTimeMillis());
            }
            rec.setError(error);
            se.getRecords().add(rec);

            if (!timedOut && error == null) {
                finishStepSuccess(inst, se, compensate, result, attempt,
                        "attempt " + attempt + " succeeded",
                        compensate ? TraceType.STEP_COMPENSATED : TraceType.STEP_SUCCEEDED);
                return true;
            }

            if (compensate) {
                se.setCompensationAttempts(attempt);
            } else {
                se.setAttempts(attempt);
            }
            se.setLastError(error);
            se.setUpdatedMillis(System.currentTimeMillis());
            trace(inst, compensate ? TraceType.STEP_COMPENSATE_FAILED : TraceType.STEP_FAILED, step.name(),
                    "attempt " + attempt + " " + (timedOut ? "timed out" : "failed") + ": " + error
                            + (RetryPolicy.hasNextAttempt(attempt, maxAttempts)
                            ? " -> will retry per deterministic policy"
                            : " -> max attempts exhausted"));
            persist(inst);

            if (!RetryPolicy.hasNextAttempt(attempt, maxAttempts)) {
                se.setStatus(compensate
                        ? com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.COMPENSATION_FAILED
                        : com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.FAILED);
                se.setUpdatedMillis(System.currentTimeMillis());
                persist(inst);
                return false;
            }
            try {
                RetryPolicy.backoff(step.retryBackoffMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void finishStepSuccess(SagaInstance inst, StepExecution se, boolean compensate, Object result,
                                   int attempt, String detail, TraceType traceType) {
        if (compensate) {
            se.setCompensationAttempts(Math.max(se.getCompensationAttempts(), attempt));
            se.setStatus(com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.COMPENSATED);
        } else {
            se.setAttempts(Math.max(se.getAttempts(), attempt));
            se.setStatus(com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SUCCEEDED);
            se.setResult(result);
        }
        se.setLastError(null);
        se.setUpdatedMillis(System.currentTimeMillis());
        trace(inst, traceType, se.getName(), detail);
        persist(inst);
    }

    private ActionContext buildContext(SagaInstance inst, SagaDefinition def, StepDefinition current,
                                       int attempt, String phase) {
        Map<String, Object> results = new LinkedHashMap<>();
        for (StepExecution se : inst.getSteps()) {
            if (se.getStatus() == com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.SUCCEEDED) {
                results.put(se.getName(), se.getResult());
            }
        }
        return new ActionContext(inst.getInstanceId(), current.name(), attempt,
                inst.getBusinessId(), inst.getInput(), results,
                ActionKeys.of(inst.getType(), inst.getInstanceId(), current.name(), phase));
    }

    // ---------------------------------------------------------------------
    // 查询
    // ---------------------------------------------------------------------

    public SagaSnapshot snapshot(String instanceId) {
        return toSnapshot(require(instanceId));
    }

    public List<SagaSnapshot> list() {
        return store.findAll().stream().map(this::toSnapshot).toList();
    }

    private SagaSnapshot toSnapshot(SagaInstance inst) {
        List<SagaSnapshot.StepView> views = inst.getSteps().stream()
                .map(se -> {
                    String phase = se.getStatus() == StepStatus.COMPENSATING
                            ? ActionKeys.PHASE_COMPENSATE : ActionKeys.PHASE_FORWARD;
                    GuardRecord g = (se.getStatus() == StepStatus.RUNNING
                            || se.getStatus() == StepStatus.COMPENSATING)
                            ? store.findGuard(IdempotentActionGuard.key(inst.getInstanceId(), se.getName(), phase))
                                    .orElse(null)
                            : null;
                    boolean inFlight = g != null && IdempotentActionGuard.IN_FLIGHT.equals(g.getState());
                    return new SagaSnapshot.StepView(se.getName(), se.getIndex(), se.getStatus(),
                            se.getAttempts(), se.getCompensationAttempts(), se.getLastError(), se.getResult(),
                            List.copyOf(se.getRecords()),
                            ActionKeys.forward(inst.getType(), inst.getInstanceId(), se.getName()),
                            ActionKeys.compensation(inst.getType(), inst.getInstanceId(), se.getName()),
                            inFlight, inFlight ? g.getAttempt() : 0);
                })
                .toList();
        return new SagaSnapshot(inst.getInstanceId(), inst.getType(), inst.getBusinessId(),
                inst.getStatus(), inst.getVersion(), views, List.copyOf(inst.getTraces()),
                inst.getUpdatedMillis());
    }

    // ---------------------------------------------------------------------
    // 内部辅助
    // ---------------------------------------------------------------------

    private SagaInstance require(String instanceId) {
        return store.find(instanceId)
                .orElseThrow(() -> new SagaRejectedException(RejectReason.UNKNOWN_INSTANCE,
                        "saga instance '" + instanceId + "' does not exist"));
    }

    private void persist(SagaInstance inst) {
        inst.setUpdatedMillis(System.currentTimeMillis());
        store.update(inst, inst.getVersion());
    }

    private void transition(SagaInstance inst,
                            com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus target,
                            String reason) {
        log.info("[transition] instance={} {} -> {} | reason: {}",
                inst.getInstanceId(), inst.getStatus(), target, reason);
        inst.setStatus(target);
        TraceEvent ev = new TraceEvent();
        ev.setMillis(System.currentTimeMillis());
        ev.setType(switch (target) {
            case RUNNING -> TraceType.FORWARD_START;
            case COMPLETED -> TraceType.COMPLETED;
            case COMPENSATING -> TraceType.COMPENSATION_START;
            case COMPENSATION_FAILED -> TraceType.COMPENSATION_FAILED;
            case COMPENSATED -> TraceType.COMPENSATED_ALL;
            default -> TraceType.REJECTED;
        });
        ev.setDetail(inst.getStatus() + " : " + reason);
        inst.getTraces().add(ev);
        persist(inst);
    }

    private void trace(SagaInstance inst, TraceType type, String stepName, String detail) {
        TraceEvent ev = new TraceEvent();
        ev.setMillis(System.currentTimeMillis());
        ev.setType(type);
        ev.setStepName(stepName);
        ev.setDetail(detail);
        inst.getTraces().add(ev);
        log.info("[trace] instance={} step={} event={} | {}", inst.getInstanceId(), stepName, type, detail);
    }

    private String serialize(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            return null;
        }
    }

    private Object deserialize(String json) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readValue(json, Object.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur.getClass().getSimpleName() + ": " + cur.getMessage();
    }

}
