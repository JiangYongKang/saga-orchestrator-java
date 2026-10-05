# Saga Orchestrator（本地可验证的分布式事务编排组件）

一个**不依赖任何外部中间件**的 Saga 编排组件：用本地文件存储替代数据库 / 消息队列，
在单机、单测内即可验证「正向执行、失败补偿、重复触发、并发提交、重启恢复」等分布式事务场景，
并对每次输入、状态迁移与判定依据给出可解释的状态与轨迹。

技术栈沿用工程现状：**Java 21 + Spring Boot 3.4 + Spring Web + JUnit 5 + Maven**。

---

## 1. 快速开始

```bash
# 运行全部测试（19 个，含单元、持久化、在途崩溃恢复、Spring 集成测试），无需任何外部服务
mvn test

# 只跑编排器核心用例（可看到完整的输入/迁移/判定日志）
mvn test -Dtest=SagaOrchestratorTest
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

### 5.2 稳定去重标识 `dedupKey`（业务侧判重抓手）

每次调用业务动作（正向或补偿）时，`ActionContext.dedupKey()` 都会带上一个**稳定去重标识**：

- 取值与幂等守卫同键：`{instanceId}|{stepName}|{FORWARD|COMPENSATE}`，纯确定性推导；
- **跨重试、跨重启、跨重复触发保持不变**——同一逻辑动作第 1 次尝试和第 N 次恢复重放，
  业务方看到的都是同一个键；
- 不同步骤、同一步骤的正向与补偿、不同实例之间**互不撞车**；
- 快照中每步的 `forwardDedupKey` / `compensationDedupKey` 可直接查询，便于对账。

业务方用法：把 `dedupKey` 当作自己副作用的幂等键（唯一约束 / 去重表 / putIfAbsent）。
当「副作用已落地但确认没来得及落盘」（崩溃、超时）时，编排器会用同一个键重放，
业务侧凭键认出「这是同一次动作的重放」，直接返回之前的结果即可挡掉重复生效。

**兼容范围（对老实例的行为）**：键是从 `instanceId + stepName + phase` 现场推导的，
不依赖任何新增持久化字段，因此**升级前已落盘的老实例无需迁移**，恢复时自动获得同样的键。
唯一限制：若业务侧在升级前没有记录过这些键，则对升级前已经生效、但确认丢失的个别动作，
业务侧无从 retroactive 判重——这与任何幂等键方案的语义一致；升级后新下发的动作全部带键。

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

## 7. 重启 / 重新初始化恢复（含在途崩溃）

- 编排器所有状态（实例、步骤、轨迹、守卫）均在每次状态迁移时原子写盘（临时文件 + rename）。
- `recoverAll()`（Spring 启动时由 `SagaStartupRecovery` 自动执行）扫描持久化目录，
  对 `PENDING/RUNNING/COMPENSATING/COMPENSATION_FAILED` 的实例从断点继续：
  - `SUCCEEDED` 步骤跳过、`COMPENSATED` 补偿跳过 —— 不重复执行已确认工作；
  - `COMPENSATION_FAILED` 的补偿会被再次尝试 —— 不跳过错过的补偿；
  - 守卫记录随磁盘恢复，`DONE` 仍然短路。
- **恢复与定义加载顺序无关**：若恢复扫描时某类型尚未注册定义，该实例先跳过并告警；
  之后 `register()` 注册该类型时会**自动补扫并恢复**该类型的所有未完成实例，
  保证无论自动恢复先跑还是定义先加载，都不会漏掉任何应继续推进的实例。
- **恢复幂等**：`recoverAll()` 可重复调用、可并发调用（含多个进程/编排器实例指向同一存储目录）。
  竞争同一实例的恢复由「守卫 fencing + 乐观版本」收敛：胜出者推进状态，
  竞争失败方的过期写入被 `VERSION_CONFLICT` 拒绝并记录日志，不产生额外副作用。

### 7.1 在途崩溃恢复规则（动作已下发、结果未确认）

崩溃瞬间守卫记录停留在 `IN_FLIGHT`（动作已下发、结果未知）。重启后按守卫状态分三种收敛：

| 崩溃现场（磁盘上的守卫 / 步骤状态） | 恢复处置 | 副作用保证 |
| --- | --- | --- |
| `IN_FLIGHT` / 步骤 `RUNNING`（或 `COMPENSATING`） | 以**同一 `dedupKey` 重放**该动作，轨迹写 `IN_FLIGHT_RECOVERED` 说明判定依据 | 业务侧凭 `dedupKey` 判重，已生效的不会再生效第二次 |
| `DONE` / 步骤仍 `RUNNING`（确认已落盘、实例状态没落盘） | **不重放**，直接采用守卫中已确认的结果把步骤置为成功 | 已确认的动作绝不重复调用 |
| `FAILED` / 步骤 `FAILED` | 按确定重试策略继续（未耗尽时）或走补偿 | 与正常失败路径一致 |

补偿链路同样适用：补偿动作在途崩溃后，恢复会重放该补偿（同一 `dedupKey`），
已生效的补偿由业务侧判重挡掉，**未完成的补偿不会因为重启被跳过**。

恢复后实例收敛到唯一终态，快照中实例状态、步骤状态与守卫结论互相一致；
每一次在途处置都能在轨迹中找到对应事件与原因。

配置项（`application.properties`）：

```properties
saga.store-dir=target/saga-store   # 本地持久化目录
saga.recover-on-startup=true       # 启动是否自动恢复
```

---

## 8. 可解释查询

`SagaSnapshot` 包含：实例状态、版本、每步状态 / 尝试次数 / 补偿次数 / 最近错误 / 步骤结果 /
每次尝试记录（`AttemptRecord`：阶段、序号、SUCCESS/FAILURE/TIMEOUT、错误、起止时间），
以及按时间排列的完整 `TraceEvent` 轨迹（提交、去重、开始、重试、超时、成功、失败、
补偿开始 / 重试 / 成功 / 失败、整体完成 / 补偿完成、恢复、在途重放、拒绝）。

每个步骤视图 `StepView` 额外暴露：

| 字段 | 含义 |
| --- | --- |
| `forwardDedupKey` / `compensationDedupKey` | 正向 / 补偿动作的稳定去重标识（见 5.2） |
| `inFlightPhase` | `FORWARD` / `COMPENSATE`：该阶段动作已下发、结果未确认（在途中间态）；`null` 表示无悬空动作 |

崩溃后、恢复前查询快照，可直接凭 `inFlightPhase` 看出「哪一步悬在未知结果上」；
恢复后通过轨迹中的 `IN_FLIGHT_RECOVERED` 事件（含所用 `dedupKey`）解释处置依据。

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
| 正向动作在途崩溃：重放同键、副作用不重复、恢复快照/轨迹可解释 | `InFlightCrashRecoveryTest.forwardInFlightCrashRecoversToUniqueTerminalState` |
| 补偿动作在途崩溃：补偿被重放而非跳过、不重复生效 | `InFlightCrashRecoveryTest.compensationInFlightCrashIsReplayedNotSkipped` |
| 确认已落盘但实例状态未落盘：采用守卫结论、绝不重放 | `InFlightCrashRecoveryTest.confirmedButUnpersistedEffectIsAdoptedWithoutReexecution` |
| 多进程并发恢复 + 重复恢复：结论一致、副作用仅一次 | `InFlightCrashRecoveryTest.concurrentAndRepeatedRecoveryStaysIdempotent` |
| 去重标识跨重试/重启稳定、跨动作不撞车 | `InFlightCrashRecoveryTest.dedupKeyIsStableAcrossRetriesAndDistinctAcrossActions` |
| 创建去重、版本冲突、磁盘扫描恢复 | `FileSagaStoreTest` |
| Spring 装配、HTTP 提交/查询/补偿、拒绝码、端到端补偿 | `SagaIntegrationTest` |

测试日志会打印每次输入（`[input]`/`[submit]`）、状态迁移（`[transition]`）、
守卫判定（`[guard] ... previous=...`）、业务侧判重（`[business] ... APPLIED/SUPPRESSED`）
与断言依据（`判定依据：...`）。

### 在本地复现在途崩溃恢复

```bash
# 只跑在途崩溃恢复用例，日志可见：崩溃现场 -> 在途快照 -> 重放判重 -> 唯一终态
mvn test -Dtest=InFlightCrashRecoveryTest
```

手工复现（正向链路）：

```bash
# 1. 用一个会挂起的动作提交实例（模拟副作用生效后进程崩溃），随后杀掉进程；
#    存储目录（默认 target/saga-store）里此时是：实例 RUNNING + 守卫 IN_FLIGHT。
# 2. 重启应用（saga.recover-on-startup=true），或调用：
#      GET  /saga/{instanceId}   # 恢复前：steps[0].inFlightPhase == "FORWARD"
#      POST 任意触发 recoverAll()（Java API），或重新 register 该类型定义
# 3. 再次 GET /saga/{instanceId}：实例收敛到 COMPLETED，
#    traces 中含 IN_FLIGHT_RECOVERED（带 dedupKey），业务侧日志可见重放被判重挡掉。
```
