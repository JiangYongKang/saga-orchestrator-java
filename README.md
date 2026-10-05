# Saga Orchestrator（本地可验证的分布式事务编排组件）

一个**不依赖任何外部中间件**的 Saga 编排组件：用本地文件存储替代数据库 / 消息队列，
在单机、单测内即可验证「正向执行、失败补偿、重复触发、并发提交、重启恢复、
在途崩溃恢复（动作已下发、确认未落盘时进程被杀）」等分布式事务场景，
并对每次输入、状态迁移与判定依据给出可解释的状态与轨迹。

技术栈沿用工程现状：**Java 21 + Spring Boot 3.4 + Spring Web + JUnit 5 + Maven**。

---

## 1. 快速开始

```bash
# 运行全部测试（20 个，含单元、持久化、崩溃恢复、Spring 集成测试），无需任何外部服务
mvn test

# 只跑编排器核心用例（可看到完整的输入/迁移/判定日志）
mvn test -Dtest=SagaOrchestratorTest

# 只跑在途崩溃恢复用例（动作已下发、确认未落盘时进程被杀）
mvn test -Dtest=SagaCrashRecoveryTest
```

默认持久化目录为 `target/saga-store/`（可用配置项覆盖，见第 7 节），
所有实例与幂等守卫记录均以 JSON 原子落盘，删除该目录即可重置。

### 以 Java API 使用

```java
SagaOrchestrator orch = new SagaOrchestrator(new FileSagaStore(Path.of("target/saga-store")));

orch.register(new SagaDefinition("order", List.of(
    new StepDefinition("reserve",
        ctx -> reserve(ctx.businessId()),          // 正向动作（建议天然幂等）
        ctx -> releaseReserve(ctx.businessId()),   // 补偿动作（必须幂等）
        3,        // maxAttempts：正向最大尝试次数（含首次）
        0L,       // timeoutMillis：单次尝试超时，<=0 不限
        50L),     // retryBackoffMillis：固定退避
    new StepDefinition("pay", ctx -> pay(ctx.businessId()), ctx -> refund(ctx.businessId()))
)));

SubmitResult r = orch.submit("order", "order-1", "BIZ-1001", Map.of("amount", 100));
r.snapshot().status();       // COMPLETED / COMPENSATED / COMPENSATION_FAILED ...
r.deduplicated();            // 是否为重复/并发提交的去重返回
orch.snapshot("order-1");    // 可解释快照：每步状态 + 完整轨迹
orch.triggerCompensation("order-1"); // 补偿失败后手动重试（幂等）
orch.recoverAll();           // 重启/重新初始化后恢复未完成实例
```

---

## 2. 包结构

| 包 | 职责 |
| --- | --- |
| `saga.model` | 状态枚举（`SagaStatus`/`StepStatus`/`TraceType`）、`SagaDefinition`、`SagaInstance`、`StepExecution`、快照与轨迹 |
| `saga.store` | `SagaStore` 存储抽象、`FileSagaStore`（本地 JSON 文件 + 乐观版本）、`IdempotentActionGuard`（动作级幂等守卫） |
| `saga.engine` | `SagaOrchestrator`（状态机驱动核心）、`RetryPolicy`（确定重试策略） |
| `saga.exception` | `SagaRejectedException` + `RejectReason`（可区分拒绝原因） |
| `saga.config` | Spring 自动装配、配置属性、启动恢复 Runner |
| `saga.web` | HTTP 入口与统一拒绝码异常处理 |

---

## 3. 编排状态机

### 3.1 实例状态 `SagaStatus`

```
                 submit                 step 重试耗尽
 PENDING ───────────────────► RUNNING ────────────────► COMPENSATING
    │                            │                          │   │
    │ （重启恢复 RUNNING/PENDING   │ 全部步骤成功              │   │ 所有需补偿步骤成功
    │  时从断点继续，不重复执行     ▼                          │   ▼
    │                       COMPLETED（终态）                │ COMPENSATED（终态）
    │                                                       ▼
    └──────────────────────────────────────────────► COMPENSATION_FAILED
                       triggerCompensation / 恢复 ◄────┘ （中间状态完整保留，可反复重试）
```

- **终态**：`COMPLETED`、`COMPENSATED`。重复提交到终态实例：直接收敛返回，不执行任何动作。
- **非终态**：`PENDING`、`RUNNING`、`COMPENSATING`、`COMPENSATION_FAILED`。
  重新初始化后 `recoverAll()` 会从断点继续；对 `COMPENSATED` 重复触发补偿为幂等 no-op；
  对 `COMPLETED` 等不允许补偿的状态发起补偿，以 `ILLEGAL_STATE` 明确拒绝。

### 3.2 步骤状态 `StepStatus`

| 状态 | 含义 |
| --- | --- |
| `PENDING` | 尚未执行（待补偿/待执行都可在快照中直接区分） |
| `RUNNING` / `COMPENSATING` | 正向执行中 / 补偿执行中 |
| `SUCCEEDED` | 正向已确认成功，**恢复时绝不重复执行** |
| `FAILED` | 正向重试耗尽；该步骤无已确认副作用，不参与补偿 |
| `COMPENSATION_FAILED` | 补偿重试耗尽，中间状态保留，等待再次触发补偿 |
| `COMPENSATED` | 补偿已确认成功，**重复补偿绝不重复生效** |
| `SKIPPED` | 因前序步骤失败而未执行（含其补偿：无需补偿，亦标记跳过） |

---

## 4. 补偿规则

1. 某步骤的所有重试耗尽后：该步骤置 `FAILED`，其后尚未执行的步骤置 `SKIPPED`，
   实例进入 `COMPENSATING`，对**此前已 `SUCCEEDED`** 的步骤**严格逆序**补偿。
2. `FAILED` 步骤本身没有已确认的正向副作用，不补偿；`PENDING/SKIPPED` 步骤也不补偿。
3. 未登记补偿动作的步骤视为「无需补偿」，直接置 `COMPENSATED` 并记录轨迹。
4. 单个补偿失败**不会中断**整轮补偿：失败步骤置 `COMPENSATION_FAILED`，继续逆序处理其余步骤，
   实例最终置 `COMPENSATION_FAILED`，全部中间状态（含每次尝试的错误）落盘保留，绝不丢失。
5. `triggerCompensation` 对 `COMPENSATION_FAILED`/`COMPENSATING` 实例重试补偿：
   已 `COMPENSATED` 的步骤幂等跳过，只重试 `COMPENSATION_FAILED` 的步骤；全部成功后收敛为 `COMPENSATED`。

---

## 5. 幂等规则（为什么重试 / 超时不会重复生效）

### 5.1 三层幂等

1. **提交幂等**：同一 `instanceId` 的重复/并发提交，`createIfAbsent` 原子判定唯一创建者，
   再加每实例执行锁串行化，收敛到同一终态；非创建者返回 `deduplicated=true` 并追加 `SUBMIT_DEDUP` 轨迹。
2. **动作级幂等守卫 `IdempotentActionGuard`**：以 `instanceId|step|FORWARD|COMPENSATE` 为键落盘，
   状态机为 `IN_FLIGHT → DONE / FAILED`。已 `DONE` 直接短路，业务动作不会被再次调用。
3. **尝试令牌（fencing token）**：每次尝试带单调递增 `attempt`；超时后旧线程的迟到成功/失败回调，
   若 `attempt` 与当前守卫不符则被丢弃，不能污染新尝试的状态。

> 说明：超时后线程是否真的“没生效”在单机 JVM 内无法由编排器判定（线程可能仍在运行）。
> 因此业务动作本身仍应**按业务键保证幂等**；守卫保证“已确认的结果绝不重复调用”，
> 令牌保证“迟到的旧结果绝不覆盖新状态”。

### 5.2 稳定动作去重标识（action key）

每个正向动作与补偿动作都有一个**稳定去重标识**，通过 `ActionContext.actionKey()` 交给业务实现，
派生规则公开在 `saga.model.ActionKeys`：

```
saga:<type>:<instanceId>:<stepName>:<FORWARD|COMPENSATE>
```

- **稳定**：标识完全由已持久化的字段确定性派生，同一逻辑动作无论重试多少次、
  重启多少次、被重复触发多少次，标识都不变；
- **不撞车**：不同实例 / 不同步骤 / 同一步骤的正向与补偿，标识两两不同；
- **业务判重抓手**：编排器无法替业务判定「崩溃前副作用到底有没有落地」，
  因此结果未知的动作会被**以同一个 actionKey 重放**；业务方以该 key 做判重
  （例如以它为唯一键写去重表 / 账本），即可自己挡掉重复生效：

```java
new StepDefinition("debit", ctx -> {
    String key = ctx.actionKey();          // 跨重试/跨重启不变
    if (ledger.exists(key)) return ledger.resultOf(key); // 已生效过：直接返回原结果
    ledger.apply(key, doDebit(ctx.businessId()));        // 首次：真正生效并记录
    return ledger.resultOf(key);
}, ...);
```

- **兼容范围**：标识是**现场派生**的，不向磁盘写入任何新字段，因此功能上线前
  已落盘的老实例**无需迁移**——恢复时按同一规则派生即可（见
  `SagaCrashRecoveryTest.legacyPersistedInstanceFromBeforeThisFeatureStillRecovers`）。
  唯一前提：老实例的 `type / instanceId / stepName` 不变（同名定义重复注册本就被
  `CONFLICTING_DEFINITION` 约束）。

### 5.3 重试与超时

- 是否还有下一次尝试仅由 `usedAttempts < maxAttempts` 决定，退避为固定时长（无随机抖动），结论确定可复现。
- 超时（`TimeoutException`）与业务异常分别记录为 `TIMEOUT` / `FAILURE`，在步骤的 `records` 中可区分。
- 超时尝试计入尝试次数；达到 `maxAttempts` 仍未成功即判定该步骤失败。

---

## 6. 并发控制与拒绝原因

- 每实例一把可重入执行锁：同一实例的提交、补偿、恢复串行化；不同实例互不阻塞。
- 乐观版本号：实例每次落盘 `version+1`，过期写入抛 `VERSION_CONFLICT`（HTTP 409）。
- 所有被拒绝操作抛 `SagaRejectedException`，携带可枚举区分的 `RejectReason`：

| `RejectReason` | 触发情形 | HTTP |
| --- | --- | --- |
| `UNKNOWN_INSTANCE` | 操作/查询不存在的实例 | 404 |
| `ILLEGAL_STATE` | 非法状态跳转（如对 COMPLETED 发起补偿）、提交未注册类型 | 422 |
| `CONCURRENT_CONFLICT` | 同一实例正被其他执行占用（保留给并发互斥场景） | 409 |
| `VERSION_CONFLICT` | 乐观版本不匹配的过期写入 | 409 |
| `CONFLICTING_DEFINITION` | 同名类型重复注册但步骤结构不同；同一实例以不同类型提交 | 422 |

---

## 7. 重启 / 重新初始化恢复

- 编排器所有状态（实例、步骤、轨迹、守卫）均在每次状态迁移时原子写盘（临时文件 + rename）。
- `recoverAll()`（Spring 启动时由 `SagaStartupRecovery` 自动执行）扫描持久化目录，
  对 `PENDING/RUNNING/COMPENSATING/COMPENSATION_FAILED` 的实例从断点继续：
  - `SUCCEEDED` 步骤跳过、`COMPENSATED` 补偿跳过 —— 不重复执行已确认工作；
  - `COMPENSATION_FAILED` 的补偿会被再次尝试 —— 不跳过错过的补偿；
  - 守卫记录随磁盘恢复，`DONE` 仍然短路。
- 若某类型在当前进程未注册定义，则跳过并告警，同时把该类型记入「待补定义」集合；
  **定义随后注册时会自动补恢复**——无论自动恢复与业务定义加载谁先谁后，
  都不会漏掉任何应继续推进的实例（注册后再次手动 `recoverAll()` 也依然幂等）。

### 7.1 在途崩溃恢复（动作已下发、结果未确认）

最危险的崩溃窗口是：业务动作已经下发、守卫记录已落盘为 `IN_FLIGHT`，
但动作结果（成功/失败）还没来得及确认落盘，进程就被杀了。规则如下：

1. **识别**：恢复（`recoverAll` / 定义补齐后的自动补恢复 / 重复提交触发的 `resume`）
   在进入状态机前，若发现某步停留在 `RUNNING`/`COMPENSATING` 且对应守卫记录为
   `IN_FLIGHT`，即判定为「在途崩溃」——此时本进程没有任何线程在执行该实例
   （实例锁保证），该尝试一定是上一进程生命的孤儿。
2. **处置**：写一条 `IN_FLIGHT_RECOVERED` 轨迹说明判定依据，然后**以同一个
   稳定 actionKey 重放该逻辑动作**。结果未知意味着副作用可能已落地也可能没有，
   编排器不能假装它成功（会丢补偿），也不能假装它失败（会丢确认），唯一安全的
   收敛方式就是重放 + 业务按键判重。
3. **收敛保证**：
   - 真正生效过的业务操作不会第二次生效——业务用 `actionKey` 认出重放并短路；
   - 没做完的补偿不会因为这次重启被跳过——`COMPENSATING` 中的补偿同样按上述规则重放；
   - 恢复幂等——`recoverAll()` 在实例锁内重读状态，已被并发恢复推进到终态的实例
     直接收敛（不计数、不追加矛盾轨迹）；自动恢复后再手动恢复、多个恢复并发跑，
     副作用都不会增多，轨迹中只出现一次恢复结论。
4. **可解释**：快照的每个步骤视图带 `actionKey` / `compensationKey` / `inFlight` /
   `inFlightAttempt` 字段。`inFlight=true` 即「动作已下发、结果还没确定」的在途
   中间态（崩溃遗留或正在执行都可据此识别）；恢复完成后该标记清零，
   不会出现「步骤 RUNNING 但实例已终态」这类矛盾结论。

配置项（`application.properties`）：

```properties
saga.store-dir=target/saga-store   # 本地持久化目录
saga.recover-on-startup=true       # 启动是否自动恢复
```

---

## 8. 可解释查询

`SagaSnapshot` 包含：实例状态、版本、每步状态 / 尝试次数 / 补偿次数 / 最近错误 / 步骤结果 /
稳定去重标识（`actionKey` / `compensationKey`）/ 在途标记（`inFlight` / `inFlightAttempt`）/
每次尝试记录（`AttemptRecord`：阶段、序号、SUCCESS/FAILURE/TIMEOUT、错误、起止时间），
以及按时间排列的完整 `TraceEvent` 轨迹（提交、去重、开始、重试、超时、成功、失败、
补偿开始 / 重试 / 成功 / 失败、整体完成 / 补偿完成、恢复、在途崩溃处置、拒绝）。

HTTP 接口（`SagaController`）：

| 方法与路径 | 说明 |
| --- | --- |
| `POST /saga/{type}/{instanceId}` | 提交；body：`{"businessId":"...","input":{...}}`；重复提交返回 `deduplicated=true` |
| `POST /saga/instances/{instanceId}/compensation` | 触发/重试补偿（幂等） |
| `GET  /saga/{instanceId}` | 查询状态与完整轨迹 |
| `GET  /saga` | 列出全部实例快照 |

被拒绝时统一返回：`{"rejected":true,"reason":"<RejectReason>","message":"..."}`。

---

## 9. 本地验证场景与对应测试

| 场景 | 测试 |
| --- | --- |
| 正常顺序完成、步骤结果传递 | `SagaOrchestratorTest.completesAllStepsInOrder` |
| 中途失败 → 逆序补偿；补偿失败保留中间态、可重试、幂等 | `SagaOrchestratorTest.compensatesInReverseOrderAndRetriesFailedCompensation` |
| 重复提交收敛唯一终态、动作只生效一次 | `SagaOrchestratorTest.duplicateSubmissionConvergesToSameTerminalState` |
| 8 线程并发提交同一实例 | `SagaOrchestratorTest.concurrentSubmissionsConvergeToSingleTerminalState` |
| 确定重试次数 | `SagaOrchestratorTest.retriesWithDeterministicPolicyAndTimeoutCountsAsAttempt` |
| 超时计入尝试、重试成功、TIMEOUT/FAILURE 可区分 | `SagaOrchestratorTest.timeoutIsRetriedAndEffectivelyCompletesOnce` |
| 重新初始化后恢复：不重复已确认工作、不跳过补偿 | `SagaOrchestratorTest.recoversAcrossReinitialisationWithoutRepeatingConfirmedWork` |
| 五类拒绝原因可区分 | `SagaOrchestratorTest.rejectsWithDistinguishableReasons` |
| 正向动作在途崩溃：重放不重复生效、收敛唯一终态、快照/轨迹可解释 | `SagaCrashRecoveryTest.forwardActionCrashMidFlightRecoversWithoutDoubleEffect` |
| 补偿动作在途崩溃：补偿既不跳过也不重复生效 | `SagaCrashRecoveryTest.compensationCrashMidFlightIsNeitherSkippedNorDoubled` |
| 恢复扫描先于定义注册：定义补齐后自动补恢复，不漏实例 | `SagaCrashRecoveryTest.recoveryScanBeforeDefinitionRegistrationStillRecovers` |
| 并发 + 重复恢复幂等：副作用不增多、结论不矛盾 | `SagaCrashRecoveryTest.concurrentAndRepeatedRecoveryStaysIdempotent` |
| 稳定去重标识：跨重试不变、不同动作互不撞车 | `SagaCrashRecoveryTest.actionKeyIsStableAcrossRetriesAndUniquePerAction` |
| 老格式落盘实例免迁移恢复 | `SagaCrashRecoveryTest.legacyPersistedInstanceFromBeforeThisFeatureStillRecovers` |
| 创建去重、版本冲突、磁盘扫描恢复 | `FileSagaStoreTest` |
| Spring 装配、HTTP 提交/查询/补偿、拒绝码、端到端补偿 | `SagaIntegrationTest` |

测试日志会打印每次输入（`[input]`/`[submit]`）、状态迁移（`[transition]`）、
守卫判定（`[guard] ... previous=...`）与断言依据（`判定依据：...`）。

### 9.1 本地复现在途崩溃

`SagaCrashRecoveryTest` 用纯本地手段复现「动作已下发、确认未落盘时进程被杀」：

1. 第一个编排器的业务动作在副作用生效后**永久阻塞**（确认永远来不及落盘），
   随后丢弃该编排器——等价于进程被杀；此时磁盘上留有 `RUNNING` 步骤 +
   `IN_FLIGHT` 守卫记录，可用 `snapshot()` 直接观察到 `inFlight=true` 的在途中间态；
2. 同一持久化目录新建编排器（= 重启），`recoverAll()` 识别在途崩溃、
   写 `IN_FLIGHT_RECOVERED` 轨迹、以同一 actionKey 重放；
3. 业务侧用 `BusinessLedger`（以 actionKey 为键的去重账本）挡掉重复生效，
   断言副作用恰好一次、终态唯一、轨迹无矛盾。

运行 `mvn test -Dtest=SagaCrashRecoveryTest` 即可在日志中完整看到
输入、状态迁移与每一步判定依据。
