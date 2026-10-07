# idempotent-write-gateway-java

写入接口的幂等执行与在途请求合并网关。在服务端统一收敛客户端重试与并发重复提交，
保证同一作用域内同一请求键只产生一次下游副作用。

## 幂等语义

- **作用域隔离**：请求键按 `(caller, key)` 二元组隔离。不同调用方使用相同的 key 互不影响；
  同一调用方内同一 key 的重复提交只产生一次副作用。
- **在途合并**：键相同且执行未完成时，后到的请求并入同一次执行，等待并复用该结果，
  不会各自再次触发下游。**合并状态只取决于执行是否结束，与执行耗时和结果有效期无关**：
  在途执行的键永不过期，执行耗时超过 `entry-ttl` 时同键请求仍然合并，
  后台清理也不会移除在途条目；只有执行结束且结果缓存到期后，键才允许过期、被清理或淘汰。
- **一致的失败语义**：一次执行失败时，所有等待方拿到一致的失败结果。
  - **可重试失败**（异常实现 `RetryableFailure` 标记接口，可由 `FailurePolicy` Bean 定制）：
    键被立即释放，后续请求可重新触发执行。
  - **不可重试失败**：失败结果在有效期内被缓存，期间重复提交直接重放失败，不会反复触发下游。
- **有效期**：每个键按 `idempotency.entry-ttl` 过期，过期后同一键可重新执行；
  后台清理任务按 `idempotency.cleanup-interval` 周期移除过期条目，长期运行内存占用有界
  （另受 `idempotency.max-entries` 硬上限约束）。
- **等待与取消**：合并等待方按 `idempotency.wait-timeout` 设置等待上限；超时或调用方线程中断
  只影响该等待方（返回 504 / 中断异常），不影响正在进行的那次执行，也不会遗留卡住的中间状态。
- **并发上限与背压**：在途执行数（`max-in-flight`）、单键等待数（`max-waiters-per-key`）、
  总键数（`max-entries`）均有上限；超限请求被立即拒绝（HTTP 429，`CAPACITY_EXCEEDED`），
  所有容量检查均为非阻塞获取，高负载下不会死锁。
- **拒绝可观测**：被容量拒绝的请求会留下独立的拒绝记录，运维接口中以 `REJECTED` 状态
  可查（含拒绝原因）。拒绝记录与键状态完全隔离，不参与合并决策，不影响同键后续请求的
  正常处理；按 `idempotency.rejection-ttl` 过期后不再可见并被后台清理移除，
  记录数量同样受 `max-entries` 约束，内存占用有界。

## 配置项

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `idempotency.entry-ttl` | `10m` | 结果/失败缓存有效期，从执行结束时起算，过期后同一键可重新执行。与执行耗时无关：在途执行永不过期 |
| `idempotency.wait-timeout` | `30s` | 合并等待方的等待上限，超时返回 504，只影响等待方本身 |
| `idempotency.cleanup-interval` | `1m` | 过期条目清理任务的执行间隔 |
| `idempotency.max-in-flight` | `64` | 同时在途执行的最大请求数，超出返回 429 |
| `idempotency.max-waiters-per-key` | `256` | 单键最大等待方数量，超出返回 429 |
| `idempotency.max-entries` | `10000` | 内存中保留的最大键数量，超出后新键返回 429；同时约束拒绝记录数量 |
| `idempotency.rejection-ttl` | `1m` | 容量拒绝记录在运维视图中的保留时长，过期后不再可见并被清理 |

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
  （`IN_FLIGHT` / `COMPLETED` / `FAILED` / `REJECTED`，含等待方数量、过期时间、失败/拒绝信息）。
- `GET /api/idempotency/keys/{caller}/{key}`：查看指定键状态，不存在返回 404。
- `DELETE /api/idempotency/keys/{caller}/{key}`：主动清理指定键状态。

## 核心组件

- `IdempotencyGateway`：幂等执行与在途合并核心。`execute(caller, key, action)` 在
  `ConcurrentHashMap.compute` 中原子地完成角色决策（执行者 / 等待者 / 命中缓存），
  执行者通过 `CompletableFuture` 向等待方广播结果；等待方带超时等待，退出时释放等待名额。
- `FailurePolicy`：失败分类策略 Bean，默认按 `RetryableFailure` 标记接口判断，可覆盖定制。
- `IdempotencyProperties`：上述全部配置项（前缀 `idempotency`）。

## 测试

```bash
mvn test
```

覆盖场景：并发重复提交只触发一次下游、调用方间键隔离、失败一致性传播、可重试失败重新触发、
不可重试失败不重放、等待超时不影响在途执行、过期后重新执行与清理、在途/等待并发上限拒绝、
运维接口查看与清理，以及 Web 层端到端重复提交与失败语义。

本次完善的场景也有对应自动化测试，可单独运行验证：

```bash
mvn test -Dtest=IdempotencyGatewayTest
```

- `inFlightExecutionMergesEvenWhenExecutionExceedsEntryTtl`：执行耗时超过 `entry-ttl` 时，
  在途键不过期、不被清理，同键请求仍合并，下游只被触发一次。
- `capacityRejectionIsObservableAndDoesNotAffectSubsequentRequests`：容量拒绝后可通过
  运维查看能力查询到 `REJECTED` 状态及拒绝原因，且不影响同键后续请求的正常处理。
- `rejectionRecordExpiresAndIsCleanedUp`：拒绝记录按 `rejection-ttl` 到期后不再可见，
  并被后台清理移除，不会长期堆积。
