# idempotent-write-gateway-java

写入接口的幂等执行与在途请求合并网关。在服务端统一收敛客户端重试与并发重复提交，
保证同一作用域内同一请求键只产生一次下游副作用。

## 幂等语义

- **作用域隔离**：请求键按 `(caller, key)` 二元组隔离。不同调用方使用相同的 key 互不影响；
  同一调用方内同一 key 的重复提交只产生一次副作用。
- **在途合并（与执行耗时、结果有效期完全解耦）**：键相同且执行未完成时，后到的请求并入同一次执行，
  等待并复用该结果，不会各自再次触发下游。**只要某次执行尚未结束，这个键就始终保持合并状态**：
  无论单次执行耗时多长，也无论是否超过 `idempotency.entry-ttl`，后来请求等待多久都只能复用同一次
  执行的结果。执行期间该键既不会被判定为过期，也不会被后台清理移除；结果有效期从执行**真正结束**
  的时刻才开始起算，因此“执行耗时”与“结果有效期”没有任何关系。
- **一致的失败语义**：一次执行失败时，所有等待方拿到一致的失败结果。
  - **可重试失败**（异常实现 `RetryableFailure` 标记接口，可由 `FailurePolicy` Bean 定制）：
    键被立即释放，后续请求可重新触发执行。
  - **不可重试失败**：失败结果在有效期内被缓存，期间重复提交直接重放失败，不会反复触发下游。
- **有效期与在途保护**：每个键按 `idempotency.entry-ttl` 过期，过期后同一键可重新执行；
  后台清理任务按 `idempotency.cleanup-interval` 周期移除过期条目，长期运行内存占用有界
  （另受 `idempotency.max-entries` 硬上限约束）。**在途（`IN_FLIGHT`）条目豁免过期与清理**，
  只有执行结束（成功 / 不可重试失败）并且结果缓存到期之后，才允许释放或淘汰该键。
- **等待与取消**：合并等待方按 `idempotency.wait-timeout` 设置等待上限；等待超时（504）或线程中断
  只影响该等待方本身，不影响正在进行的那次执行，也不会遗留卡住的中间状态，更不会导致下游被重复触发。
- **并发上限、背压与拒绝可观测**：在途执行数（`max-in-flight`）、单键等待数（`max-waiters-per-key`）、
  总键数（`max-entries`）均有上限；超限请求被立即拒绝（HTTP 429，`CAPACITY_EXCEEDED`），
  所有容量检查均为非阻塞获取，高负载下不会死锁。
  - 被拒绝的请求会留下一条状态为 `REJECTED` 的记录，可通过运维查看能力（`GET .../keys`、
    `GET .../keys/{caller}/{key}`）查询到，并与 `IN_FLIGHT` / `COMPLETED` / `FAILED` 明确区分；
    记录的 `failureMessage` 为拒绝原因，`expiresAt` 为其到期时刻。
  - 拒绝记录的有效期复用 `idempotency.entry-ttl`，到期后查询不再可见、并由后台清理自动移除，
    不会长期堆积造成内存无界增长。
  - 拒绝记录**仅供观测，不参与执行决策**：它不阻塞、不合并任何请求，也不影响后续同键请求的正常处理；
    一旦该键真正开始执行，早先的拒绝记录即被清除。

## 配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `idempotency.entry-ttl` | `10m` | 执行结束后结果/失败缓存的保留时长，过期后同一键可重新执行；同时作为容量拒绝记录（`REJECTED`）的有效期。在途条目不受此值约束，无需大于执行耗时 |
| `idempotency.wait-timeout` | `30s` | 合并等待方的等待上限，超时返回 504，仅影响等待方本身 |
| `idempotency.cleanup-interval` | `1m` | 过期条目（含拒绝记录）清理任务的执行间隔；在途条目永不被清理 |
| `idempotency.max-in-flight` | `64` | 同时在途执行的最大请求数，超出返回 429 |
| `idempotency.max-waiters-per-key` | `256` | 单键最大等待方数量，超出返回 429 |
| `idempotency.max-entries` | `10000` | 内存中保留的最大键数量，超出后新键返回 429 |

## HTTP 接口

### 写入（演示）

`POST /api/writes`

```json
{"caller": "app-1", "key": "order-123", "payload": "...", "delayMillis": 0, "failMode": "NONE"}
```

`failMode` 用于演示下游失败：`NONE` / `RETRYABLE`（可重试）/ `FATAL`（不可重试）。

响应：`200` 成功（重复提交复用结果）；`429` 容量拒绝；`504` 等待超时；`500` 下游失败。

### 运维

- `GET /api/idempotency/keys`：列出所有未过期键的状态
  （`IN_FLIGHT` / `COMPLETED` / `FAILED` / `REJECTED`，含等待方数量、过期时间、失败/拒绝原因）。
- `GET /api/idempotency/keys/{caller}/{key}`：查看指定键状态，不存在或已过期返回 404。
  同一键存在有效执行状态时优先展示执行状态；仅存在带有效期的拒绝记录时展示 `REJECTED`。
- `DELETE /api/idempotency/keys/{caller}/{key}`：主动清理指定键状态（含其拒绝记录）。

> 接口路径、请求/响应结构均保持不变，`REJECTED` 只是 `status` 字段的一个新枚举值，
> 已有配置项与默认行为无需迁移即可继续使用。

## 核心组件

- `IdempotencyGateway`：幂等执行与在途合并核心。`execute(caller, key, action)` 在
  `ConcurrentHashMap.compute` 中原子地完成角色决策（执行者 / 等待者 / 命中缓存），
  执行者通过 `CompletableFuture` 向等待方广播结果；等待方带超时等待，退出时释放等待名额。
  在途条目不参与过期判定与后台清理，结果有效期在执行结束时才起算；容量拒绝写入独立的、
  带 `entry-ttl` 有效期的拒绝记录表，仅供 `inspect` / `list` 观测，不参与任何执行决策。
- `FailurePolicy`：失败分类策略 Bean，默认按 `RetryableFailure` 标记接口判断，可覆盖定制。
- `IdempotencyProperties`：上述全部配置项（前缀 `idempotency`）。

## 测试与本地验证

```bash
mvn test
```

覆盖场景：并发重复提交只触发一次下游、调用方间键隔离、失败一致性传播、可重试失败重新触发、
不可重试失败不重放、等待超时不影响在途执行、过期后重新执行与清理、在途/等待并发上限拒绝、
运维接口查看与清理，以及 Web 层端到端重复提交与失败语义。

本次新增的关键场景（`IdempotencyGatewayTest` / `RejectionObservabilityWebTest`）：

1. **执行耗时超过结果有效期仍合并、下游只触发一次**
   （`executionLongerThanTtlStillMergesAndTriggersDownstreamOnce`）：
   `entry-ttl=100ms` 的执行持续远超该时长，期间时钟推进 10 分钟、后台清理多轮运行，
   在途键仍为 `IN_FLIGHT`、未被清理，后来请求作为等待方并入同一次执行；执行结束后再过一个
   TTL 才允许重新执行。
2. **容量拒绝可查询且状态可区分，且不阻塞后续同键请求**
   （`capacityRejectionIsObservableWithDistinguishableStatusAndDoesNotBlockRetry`
   与 `RejectionObservabilityWebTest`）：超限请求抛 `CapacityExceededException` / 返回 429 后，
   `inspect`、`list`（HTTP `GET /api/idempotency/keys`）能看到 `REJECTED` 状态与拒绝原因；
   容量恢复后同键请求正常执行，状态转为 `COMPLETED`。
3. **拒绝状态到期后不再可见**（`rejectedRecordDisappearsAfterTtl`）：
   推进时钟超过一个 `entry-ttl` 后，`inspect` 返回空，后台清理将其从 `list` 移除。
4. **高并发长耗时下的合并与等待隔离**
   （`burstDuringLongExecutionTriggersOnceAndWaitTimeoutDoesNotKillExecution`）：
   20 个同键并发请求在长耗时执行期间涌入，等待超时只让等待方自身收到 `WaitTimeoutException`，
   在途执行照常完成，下游全程只被触发一次。

本地用 HTTP 手工验证拒绝可观测性（将在途上限调小后制造长耗时下游）：

```bash
# 以较小容量启动
java -jar target/idempotent-write-gateway-java-1.0-SNAPSHOT.jar --idempotency.max-in-flight=1 &

# 占用唯一在途名额（下游耗时 2 秒）
curl -s -XPOST localhost:8080/api/writes -H 'Content-Type: application/json' \
  -d '{"caller":"app-1","key":"busy","payload":"p","delayMillis":2000,"failMode":"NONE"}' &

# 立即提交另一个键：返回 429
curl -i -XPOST localhost:8080/api/writes -H 'Content-Type: application/json' \
  -d '{"caller":"app-1","key":"turned-away","payload":"p","delayMillis":0,"failMode":"NONE"}'

# 运维接口可查到可区分的 REJECTED 状态（含 failureMessage 与 expiresAt）
curl -s localhost:8080/api/idempotency/keys/app-1/turned-away
curl -s localhost:8080/api/idempotency/keys
```
