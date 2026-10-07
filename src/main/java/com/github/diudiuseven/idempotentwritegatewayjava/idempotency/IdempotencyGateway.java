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
 *   <li>同一键在途时，后到请求合并等待并复用同一次执行结果。<b>只要执行尚未结束，该键就始终
 *       保持合并状态</b>，与结果有效期（{@code entry-ttl}）和单次执行耗时均无关；后来请求等待
 *       多久都只会复用同一次执行的结果，下游不会收到重复副作用。</li>
 *   <li>执行成功后结果缓存至过期；可重试失败立即释放键，不可重试失败在有效期内重放失败。
 *       结果有效期从执行真正结束时才开始计算，在途条目既不会被判定为过期，也不会被后台清理
 *       移除；只有执行结束且缓存到期后才允许释放/淘汰该键。</li>
 *   <li>等待方有等待上限，超时/中断只影响等待方本身，不影响正在进行的执行。</li>
 *   <li>在途执行数、单键等待数、总键数均有上限，超限以 {@link CapacityExceededException} 拒绝。
 *       被拒绝的请求会留下带有效期、状态为 {@link KeyStatus#REJECTED} 的可观测记录，可通过
 *       运维查看能力查询；该记录仅供观测，不参与执行决策，不影响后续同键请求，到期自动清理。</li>
 * </ul>
 */
public class IdempotencyGateway implements AutoCloseable {

    private final IdempotencyProperties properties;
    private final FailurePolicy failurePolicy;
    private final Clock clock;
    private final ConcurrentHashMap<ScopedKey, KeyState> states = new ConcurrentHashMap<>();
    /** 容量拒绝记录：仅供运维观测，带有效期，不参与 execute 的合并/缓存决策。 */
    private final ConcurrentHashMap<ScopedKey, Rejection> rejections = new ConcurrentHashMap<>();
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
            // 容量拒绝不落键状态、不阻塞后续请求；只留下一条带有效期的可观测拒绝记录
            recordRejection(scoped, e.getMessage());
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
        // 在途/结果状态优先；无有效状态时才展示最近一次容量拒绝记录
        Rejection rejection = rejections.get(scoped);
        if (rejection != null && !isRejectionExpired(rejection)) {
            return Optional.of(toView(scoped, rejection));
        }
        return Optional.empty();
    }

    /** 列出当前所有未过期键的状态（含带有效期的容量拒绝记录）。 */
    public List<KeyStateView> list() {
        List<KeyStateView> views = new ArrayList<>();
        states.forEach((scoped, state) -> {
            if (!isExpired(state)) {
                views.add(toView(state));
            }
        });
        rejections.forEach((scoped, rejection) -> {
            // 同一键存在有效执行状态时，拒绝记录不再重复展示
            if (!isRejectionExpired(rejection) && !states.containsKey(scoped)) {
                views.add(toView(scoped, rejection));
            }
        });
        return views;
    }

    /** 主动清理指定键的状态（含其拒绝记录），返回是否存在并被移除。 */
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
            // 占位值：执行期间不参与过期判定，执行结束时以结束时刻重新起算有效期
            this.expiresAtMillis = nowMillis + ttlMillis;
        }
    }

    /** 一次容量拒绝的可观测记录，仅供 inspect/list 展示，带独立有效期。 */
    private static final class Rejection {
        final long rejectedAtMillis;
        final long expiresAtMillis;
        final String reason;

        Rejection(long nowMillis, long ttlMillis, String reason) {
            this.rejectedAtMillis = nowMillis;
            this.expiresAtMillis = nowMillis + ttlMillis;
            this.reason = reason;
        }
    }

    private void recordRejection(ScopedKey scoped, String reason) {
        long now = clock.millis();
        rejections.put(scoped, new Rejection(now, properties.getEntryTtl().toMillis(), reason));
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
            // 真正执行开始：清除该键早先的容量拒绝记录，拒绝绝不阻塞后续同键请求
            rejections.remove(k);
            return fresh;
        });
        return decision;
    }

    private <T> T runAsLeader(ScopedKey scoped, KeyState state, Supplier<T> action) {
        try {
            T result = action.get();
            // 结果有效期从执行真正结束的时刻起算，与执行耗时无关
            state.expiresAtMillis = clock.millis() + properties.getEntryTtl().toMillis();
            state.result = result;
            state.status = KeyStatus.COMPLETED;
            state.future.complete(result);
            return result;
        } catch (Throwable t) {
            boolean retryable = failurePolicy.isRetryable(t);
            // 失败缓存有效期同样从执行真正结束的时刻起算
            state.expiresAtMillis = clock.millis() + properties.getEntryTtl().toMillis();
            state.failure = t;
            state.retryableFailure = retryable;
            state.status = KeyStatus.FAILED;
            state.future.completeExceptionally(t);
            if (retryable) {
                // 可重试失败：立即释放键，后续请求可重新触发
                states.remove(scoped, state);
            }
            throw propagate(t);
        } finally {
            // 本次执行已经发生，早先的拒绝记录不再具有观测意义，避免在结果缓存到期后"复活"
            rejections.remove(scoped);
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

    /**
     * 在途条目永不过期：只有执行真正结束（COMPLETED / 不可重试 FAILED）后，
     * 结果缓存才参与有效期判定。执行耗时与结果有效期完全解耦。
     */
    private boolean isExpired(KeyState state) {
        return state.status != KeyStatus.IN_FLIGHT && clock.millis() >= state.expiresAtMillis;
    }

    private boolean isRejectionExpired(Rejection rejection) {
        return clock.millis() >= rejection.expiresAtMillis;
    }

    private void cleanupSafely() {
        try {
            // 在途条目不会被清理；只有执行结束且缓存到期的条目才会被移除
            states.entrySet().removeIf(entry -> isExpired(entry.getValue()));
            rejections.entrySet().removeIf(entry -> isRejectionExpired(entry.getValue()));
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

    private KeyStateView toView(ScopedKey scoped, Rejection rejection) {
        Instant rejectedAt = Instant.ofEpochMilli(rejection.rejectedAtMillis);
        Instant expiresAt = Instant.ofEpochMilli(rejection.expiresAtMillis);
        return new KeyStateView(
                scoped.caller(),
                scoped.key(),
                KeyStatus.REJECTED,
                rejectedAt,
                expiresAt,
                0,
                rejection.reason,
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
