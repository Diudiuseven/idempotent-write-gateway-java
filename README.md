# Idempotent Write Gateway（写入幂等执行与在途合并网关）

面向写入接口的服务端幂等组件：客户端重试、并发重复提交在服务端统一收敛为一次下游副作用；
同一键的在途请求并入同一次执行，等待并复用结果，避免重复调用下游。

## 一、幂等语义

| 语义 | 说明 |
| --- | --- |
| 作用域隔离 | 幂等键 = `(callerId, key)`，按调用方隔离。不同调用方即使键相同也互不串扰；同一调用方内同一键只产生一次副作用。 |
| 在途合并 | 键相同的请求在执行未完成时，不重复触发下游，而是并入同一次执行，所有等待方复用同一个结果。 |
| 一致失败 | 一次执行失败时，所有等待方拿到**同一个失败结果**。 |
| 失败重试策略 | 失败分为**可重试**与**不可重试**（见下文）。可重试失败允许后续请求立即重新触发下游；不可重试失败在有效期内直接复用失败结果，不重放。 |
| 有效期（TTL） | 已完成 / 不可重试失败的条目在有效期内复用结果；过期后同一键可以重新执行。 |
| 等待与取消 | 合并等待有超时上限；等待超时或等待方被中断**只放弃本次等待**，不取消在途执行，也不遗留卡住的中间状态。 |
| 并发上限与背压 | 执行并发数、单键等待者数均可配置并有上限；超出容量时返回**可区分的拒绝结果**，高负载下不死锁。 |
| 清理 | 过期条目由定时任务清理 + 访问时惰性过期，长期运行内存不无限增长。 |
| 可观测 | 可查看键当前状态（执行中 / 已完成 / 失败 / 被拒绝）、等待者数、开始/完成/过期时间，并可主动查看或清理指定键。 |

### 失败分类

默认分类器（`FailureClassifier.defaultClassifier()`）规则：

- `NonRetryableException`（业务侧显式标记的不可重试异常）与 JVM 级错误（`Error`）→ **不可重试**，有效期内缓存失败结果；
- 其余异常（如下游超时、5xx 等 `RuntimeException`）→ **可重试**，后续请求可立即重新触发。

可通过自定义 `FailureClassifier` Bean 覆盖默认分类规则。

### 状态机

```
IN_PROGRESS ──成功──▶ COMPLETED（TTL 内复用结果，过期后可重新执行）
           ──可重试失败──▶ FAILED_RETRYABLE（短暂状态保留，后续请求立即重新触发）
           ──不可重试失败──▶ FAILED_NON_RETRYABLE（TTL 内复用失败结果，过期后可重新执行）
           ──容量不足──▶ REJECTED（短暂状态保留，后续请求可立即重新尝试）
```

## 二、配置项

配置前缀 `idempotency`，可在 `application.properties` / `application.yml` 中覆盖：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `idempotency.ttl` | `10m` | 已完成 / 不可重试失败条目的有效期，过期后同一键可重新执行 |
| `idempotency.wait-timeout` | `30s` | 合并等待的最长等待时间；超时仅放弃本次等待，不影响在途执行 |
| `idempotency.retryable-failure-ttl` | `1s` | 可重试失败 / 被拒绝条目的状态保留时长（仅影响观测可见性，**不阻塞重试**） |
| `idempotency.max-concurrent-executions` | `200` | 执行并发上限（同时进行的下游执行数），超出返回 `REJECTED_CAPACITY` |
| `idempotency.max-waiters` | `1000` | 单个在途键允许的最大等待者数，超出返回 `REJECTED_TOO_MANY_WAITERS` |
| `idempotency.cleanup-interval` | `30s` | 过期条目定时清理间隔 |
| `idempotency.downstream-delay` | `0s` | 演示用下游写入的模拟处理延迟（用于观察在途合并） |

## 三、HTTP API

### 写入接口

```
POST /api/writes
Idempotency-Key: <调用方内唯一键>
X-Caller-Id: <调用方标识>
Content-Type: text/plain

<写入载荷>
```

- 成功：`200 OK`，返回下游写入结果（`writeId`、`echo`、`processedAt`）。
- 缺失 `Idempotency-Key` 或 `X-Caller-Id`：`400 Bad Request`。
- 幂等相关错误响应体：

```json
{ "reason": "EXECUTION_FAILED_RETRYABLE", "retryable": true, "message": "..." }
```

HTTP 状态码映射：

| reason | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `EXECUTION_FAILED_RETRYABLE` | `500` | 下游执行失败（可重试），客户端可携带同一键重试 |
| `EXECUTION_FAILED_NON_RETRYABLE` | `422` | 下游执行失败（不可重试），有效期内重试返回同一失败 |
| `WAIT_TIMEOUT` | `408` | 等待在途执行超时（在途执行不受影响，稍后重试可能直接拿到结果） |
| `REJECTED_CAPACITY` | `503` | 执行并发已达上限，稍后重试 |
| `REJECTED_TOO_MANY_WAITERS` | `503` | 该键等待者过多，稍后重试 |
| `INTERRUPTED` | `500` | 等待线程被中断 |

### 运维接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/idempotency/keys` | 查看全部键状态，支持 `?callerId=xxx` 按调用方过滤 |
| `GET` | `/api/idempotency/keys/{callerId}/{key}` | 查看指定键状态，不存在返回 `404` |
| `DELETE` | `/api/idempotency/keys/{callerId}/{key}` | 清理指定键状态（在途执行不受影响），返回 `204`；不存在返回 `404` |

键状态字段：`key(callerId, key)`、`state`、`result`、`error`、`waiters`、
`startedAt`、`completedAt`、`expiresAt`。

## 四、核心组件

- `core.IdempotencyGateway`：幂等执行与在途合并核心（`ConcurrentHashMap` + `CompletableFuture` + 信号量）。
- `core.IdempotencyEntry`：单键状态条目与状态迁移。
- `core.FailureClassifier`：失败分类器，可自定义。
- `config.IdempotencyProperties`：配置项。
- `downstream.WriteService`：演示用下游写入服务（统计实际副作用次数）。
- `web.WriteController`：写入接口。
- `web.IdempotencyOpsController`：运维接口。

## 五、测试

```bash
mvn test
```

覆盖场景（共 25 个用例）：

- 并发重复提交合并为一次下游副作用（网关级 + HTTP 级）；
- 不同调用方作用域隔离、同调用方不同键独立执行；
- 有效期内结果复用；
- 可重试失败后重新触发、不可重试失败有效期内不重放、过期后重新执行；
- 等待超时不取消在途执行、等待者中断不影响在途执行；
- 执行并发上限拒绝（`REJECTED_CAPACITY`）且容量恢复后可重新执行；
- 等待者数量上限拒绝（`REJECTED_TOO_MANY_WAITERS`）；
- 所有等待方拿到一致失败结果；
- 执行中 / 已完成 / 被拒绝状态可观测；
- 清理在途键不影响执行、过期条目被定时清理；
- HTTP 头缺失校验、失败状态码映射、运维接口查看 / 清理 / 过滤。
