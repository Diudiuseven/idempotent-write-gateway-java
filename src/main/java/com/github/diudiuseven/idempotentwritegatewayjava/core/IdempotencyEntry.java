package com.github.diudiuseven.idempotentwritegatewayjava.core;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单个幂等键的状态条目。
 *
 * <p>状态迁移：
 * <pre>
 * IN_PROGRESS ──成功──▶ COMPLETED（有效期 ttl）
 *            ──可重试失败──▶ FAILED_RETRYABLE（短暂保留后可重新触发）
 *            ──不可重试失败──▶ FAILED_NON_RETRYABLE（有效期 ttl 内复用失败结果）
 *            ──容量不足──▶ REJECTED（短暂保留后可重新尝试）
 * </pre>
 */
public class IdempotencyEntry {

    private final IdempotencyKey key;
    private volatile EntryState state;
    private final CompletableFuture<Object> future = new CompletableFuture<>();
    private final AtomicInteger waiters = new AtomicInteger(0);
    private final Instant startedAt = Instant.now();
    private volatile Instant completedAt;
    private volatile Instant expiresAt;
    private volatile Object result;
    private volatile Throwable error;

    public IdempotencyEntry(IdempotencyKey key) {
        this.key = key;
        this.state = EntryState.IN_PROGRESS;
    }

    public IdempotencyKey getKey() {
        return key;
    }

    public EntryState getState() {
        return state;
    }

    public CompletableFuture<Object> getFuture() {
        return future;
    }

    public int incrementWaiters() {
        return waiters.incrementAndGet();
    }

    public int decrementWaiters() {
        return waiters.decrementAndGet();
    }

    public int getWaiters() {
        return waiters.get();
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public Object getResult() {
        return result;
    }

    public Throwable getError() {
        return error;
    }

    public boolean isExpired(long nowMillis) {
        Instant e = expiresAt;
        return e != null && nowMillis >= e.toEpochMilli();
    }

    /** 成功完成：结果进入缓存并设置有效期 */
    public void complete(Object result, Duration ttl) {
        this.state = EntryState.COMPLETED;
        this.result = result;
        this.completedAt = Instant.now();
        this.expiresAt = completedAt.plus(ttl);
        this.future.complete(result);
    }

    /** 执行失败：按可重试/不可重试分类，所有等待方通过 future 拿到一致的失败 */
    public void fail(Throwable error, boolean retryable, Duration retention) {
        this.state = retryable ? EntryState.FAILED_RETRYABLE : EntryState.FAILED_NON_RETRYABLE;
        this.error = error;
        this.completedAt = Instant.now();
        this.expiresAt = completedAt.plus(retention);
        this.future.completeExceptionally(error);
    }

    /** 容量不足被拒绝：条目短暂保留，等待方与后续请求拿到可区分的拒绝结果 */
    public void reject(IdempotencyException rejection, Duration retention) {
        this.state = EntryState.REJECTED;
        this.error = rejection;
        this.completedAt = Instant.now();
        this.expiresAt = completedAt.plus(retention);
        this.future.completeExceptionally(rejection);
    }
}
