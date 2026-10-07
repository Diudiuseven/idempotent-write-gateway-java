package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 幂等执行与在途请求合并的核心组件。
 *
 * <p>语义：
 * <ul>
 *   <li>键按 (caller, key) 隔离，不同调用方互不影响。</li>
 *   <li>同一键在途时，后到请求合并等待并复用同一次执行结果。</li>
 *   <li>执行成功后结果缓存至过期；可重试失败立即释放键，不可重试失败在有效期内重放失败。</li>
 *   <li>等待方有等待上限，超时/中断不影响正在进行的执行。</li>
 *   <li>在途执行数、单键等待数、总键数均有上限，超限以 {@link CapacityExceededException} 拒绝。</li>
 * </ul>
 */
public class IdempotencyGateway implements AutoCloseable {

    private final IdempotencyProperties properties;
    private final FailurePolicy failurePolicy;
    private final Clock clock;
    private final ConcurrentHashMap<ScopedKey, KeyState> states = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ScopedKey, RejectionRecord> rejections = new ConcurrentHashMap<>();
    private final Semaphore inFlightPermits;
    private final ScheduledExecutorService cleaner;

    public IdempotencyGateway(IdempotencyProperties properties, FailurePolicy failurePolicy) {
        this(properties, failurePolicy, Clock.systemUTC());
    }

    public IdempotencyGateway(IdempotencyProperties properties, FailurePolicy failurePolicy, Clock clock) {
        this.properties = properties;
        this.failurePolicy = failurePolicy;
        this.clock = clock;
        this.inFlightPermits = new Semaphore(properties.getMaxInFlight());
        this.cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "idempotency-gateway-cleaner");
            t.setDaemon(true);
            return t;
        });
        long intervalMillis = Math.max(1, properties.getCleanupInterval().toMillis());
        this.cleaner.scheduleWithFixedDelay(this::cleanupSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 以 (caller, key) 为作用域执行动作。
     */
    @SuppressWarnings("unchecked")
    public <T> T execute(String caller, String key, Supplier<T> action) {
        ScopedKey scoped = new ScopedKey(caller, key);
        Decision decision;
        try {
            decision = decide(scoped);
        } catch (CapacityExceededException e) {
            // 容量拒绝也留下可观测记录（独立存储，不影响该键后续正常处理）
            recordRejection(scoped, e);
            throw e;
        }
        return switch (decision.role) {
            case LEADER -> runAsLeader(scoped, decision.state, action);
            case WAITER -> (T) awaitResult(scoped, decision.state);
            case CACHED_SUCCESS -> (T) decision.state.result;
            case CACHED_FAILURE -> throw propagate(decision.state.failure);
        };
    }

    /** 查看指定键的当前状态。 */
    public Optional<KeyStateView> inspect(String caller, String key) {
        ScopedKey scoped = new ScopedKey(caller, key);
        KeyState state = states.get(scoped);
        if (state != null && !isExpired(state)) {
            return Optional.of(toView(state));
        }
        RejectionRecord rejection = rejections.get(scoped);
        if (rejection != null && !isExpired(rejection)) {
            return Optional.of(toView(rejection));
        }
        return Optional.empty();
    }

    /** 列出当前所有未过期键的状态（含容量拒绝记录）。 */
    public List<KeyStateView> list() {
        List<KeyStateView> views = new ArrayList<>();
        states.forEach((scoped, state) -> {
            if (!isExpired(state)) {
                views.add(toView(state));
            }
        });
        rejections.forEach((scoped, rejection) -> {
            if (!isExpired(rejection) && !viewsContain(views, scoped)) {
                views.add(toView(rejection));
            }
        });
        return views;
    }

    /** 主动清理指定键的状态，返回是否存在并被移除。 */
    public boolean evict(String caller, String key) {
        ScopedKey scoped = new ScopedKey(caller, key);
        boolean removed = states.remove(scoped) != null;
        removed |= rejections.remove(scoped) != null;
        return removed;
    }

    @Override
    public void close() {
        cleaner.shutdownNow();
    }

    // ---- 内部实现 ----

    private enum Role {LEADER, WAITER, CACHED_SUCCESS, CACHED_FAILURE}

    private static final class Decision {
        Role role;
        KeyState state;
    }

    private static final class KeyState {
        final ScopedKey scoped;
        final CompletableFuture<Object> future = new CompletableFuture<>();
        final long createdAtMillis;
        final AtomicInteger waiters = new AtomicInteger();
        volatile long expiresAtMillis;
        volatile KeyStatus status = KeyStatus.IN_FLIGHT;
        volatile Object result;
        volatile Throwable failure;
        volatile boolean retryableFailure;

        KeyState(ScopedKey scoped, long nowMillis, long ttlMillis) {
            this.scoped = scoped;
            this.createdAtMillis = nowMillis;
            this.expiresAtMillis = nowMillis + ttlMillis;
        }
    }

    /**
     * 容量拒绝的可观测记录。与键状态完全隔离存储：
     * 不参与角色决策，不影响同键后续请求的正常处理；
     * 按 {@code rejection-ttl} 过期并被清理，数量受 {@code max-entries} 约束，内存占用有界。
     */
    private static final class RejectionRecord {
        final ScopedKey scoped;
        final long createdAtMillis;
        final long expiresAtMillis;
        final String reason;

        RejectionRecord(ScopedKey scoped, long nowMillis, long ttlMillis, String reason) {
            this.scoped = scoped;
            this.createdAtMillis = nowMillis;
            this.expiresAtMillis = nowMillis + ttlMillis;
            this.reason = reason;
        }
    }

    /**
     * 在 map 的原子 compute 内决定本次调用的角色：执行者 / 等待者 / 直接命中缓存。
     */
    private Decision decide(ScopedKey scoped) {
        Decision decision = new Decision();
        states.compute(scoped, (k, existing) -> {
            if (existing != null && !isExpired(existing)) {
                if (existing.status == KeyStatus.IN_FLIGHT) {
                    if (existing.waiters.get() >= properties.getMaxWaitersPerKey()) {
                        throw new CapacityExceededException(
                                "too many waiters on key " + k + " (limit " + properties.getMaxWaitersPerKey() + ")");
                    }
                    existing.waiters.incrementAndGet();
                    decision.role = Role.WAITER;
                    decision.state = existing;
                    return existing;
                }
                if (existing.status == KeyStatus.COMPLETED) {
                    decision.role = Role.CACHED_SUCCESS;
                    decision.state = existing;
                    return existing;
                }
                // FAILED：不可重试失败在有效期内直接重放失败；可重试失败则落到下方重新执行
                if (!existing.retryableFailure) {
                    decision.role = Role.CACHED_FAILURE;
                    decision.state = existing;
                    return existing;
                }
            }
            // 成为执行者：检查容量
            if (existing == null && states.mappingCount() >= properties.getMaxEntries()) {
                throw new CapacityExceededException(
                        "too many keys (limit " + properties.getMaxEntries() + ")");
            }
            if (!inFlightPermits.tryAcquire()) {
                throw new CapacityExceededException(
                        "too many in-flight executions (limit " + properties.getMaxInFlight() + ")");
            }
            KeyState fresh = new KeyState(k, clock.millis(), properties.getEntryTtl().toMillis());
            decision.role = Role.LEADER;
            decision.state = fresh;
            return fresh;
        });
        return decision;
    }

    private <T> T runAsLeader(ScopedKey scoped, KeyState state, Supplier<T> action) {
        try {
            T result = action.get();
            state.result = result;
            state.status = KeyStatus.COMPLETED;
            state.expiresAtMillis = clock.millis() + properties.getEntryTtl().toMillis();
            state.future.complete(result);
            return result;
        } catch (Throwable t) {
            boolean retryable = failurePolicy.isRetryable(t);
            state.failure = t;
            state.retryableFailure = retryable;
            state.status = KeyStatus.FAILED;
            state.expiresAtMillis = clock.millis() + properties.getEntryTtl().toMillis();
            state.future.completeExceptionally(t);
            if (retryable) {
                // 可重试失败：立即释放键，后续请求可重新触发
                states.remove(scoped, state);
            }
            throw propagate(t);
        } finally {
            inFlightPermits.release();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T awaitResult(ScopedKey scoped, KeyState state) {
        try {
            return (T) state.future.get(properties.getWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new WaitTimeoutException(
                    "timed out waiting " + properties.getWaitTimeout() + " for in-flight key " + scoped);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IdempotencyException("interrupted while waiting for in-flight key " + scoped, e);
        } catch (ExecutionException e) {
            throw propagate(e.getCause());
        } finally {
            // 等待结束（无论成败）都释放等待名额，不影响在途执行
            state.waiters.decrementAndGet();
        }
    }

    private boolean isExpired(KeyState state) {
        // 在途执行永不过期：合并状态只与执行是否结束有关，与执行耗时和结果有效期无关。
        // 只有执行结束（COMPLETED/FAILED）且结果缓存到期后，条目才允许过期、被清理或淘汰。
        if (state.status == KeyStatus.IN_FLIGHT) {
            return false;
        }
        return clock.millis() >= state.expiresAtMillis;
    }

    private boolean isExpired(RejectionRecord record) {
        return clock.millis() >= record.expiresAtMillis;
    }

    private void recordRejection(ScopedKey scoped, CapacityExceededException cause) {
        long now = clock.millis();
        RejectionRecord record = new RejectionRecord(
                scoped, now, properties.getRejectionTtl().toMillis(), String.valueOf(cause.getMessage()));
        rejections.merge(scoped, record, (existing, fresh) -> isExpired(existing) ? fresh : existing);
        // 拒绝记录数量同样受 max-entries 约束，避免高负载下无界堆积
        if (rejections.mappingCount() > properties.getMaxEntries()) {
            rejections.entrySet().removeIf(entry -> isExpired(entry.getValue()));
        }
    }

    private static boolean viewsContain(List<KeyStateView> views, ScopedKey scoped) {
        for (KeyStateView view : views) {
            if (view.caller().equals(scoped.caller()) && view.key().equals(scoped.key())) {
                return true;
            }
        }
        return false;
    }

    private void cleanupSafely() {
        try {
            states.entrySet().removeIf(entry -> isExpired(entry.getValue()));
            rejections.entrySet().removeIf(entry -> isExpired(entry.getValue()));
        } catch (Throwable ignored) {
            // 清理任务不得因异常终止调度
        }
    }

    private KeyStateView toView(KeyState state) {
        return new KeyStateView(
                state.scoped.caller(),
                state.scoped.key(),
                state.status,
                Instant.ofEpochMilli(state.createdAtMillis),
                Instant.ofEpochMilli(state.expiresAtMillis),
                state.waiters.get(),
                state.failure == null ? null : String.valueOf(state.failure.getMessage()),
                state.retryableFailure);
    }

    private KeyStateView toView(RejectionRecord record) {
        return new KeyStateView(
                record.scoped.caller(),
                record.scoped.key(),
                KeyStatus.REJECTED,
                Instant.ofEpochMilli(record.createdAtMillis),
                Instant.ofEpochMilli(record.expiresAtMillis),
                0,
                record.reason,
                false);
    }

    private static RuntimeException propagate(Throwable t) {
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error err) {
            throw err;
        }
        return new IdempotencyException("idempotent execution failed", t);
    }
}
