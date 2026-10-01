package com.github.diudiuseven.idempotentwritegatewayjava.core;

import com.github.diudiuseven.idempotentwritegatewayjava.config.IdempotencyProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * 写入接口幂等执行与在途请求合并网关。
 *
 * <p>核心语义：
 * <ul>
 *   <li>作用域隔离：键 = (callerId, key)，不同调用方互不串扰；</li>
 *   <li>在途合并：同键请求在执行未完成时并入同一次执行，等待并复用其结果，不重复触发下游；</li>
 *   <li>一致失败：执行失败时所有等待方拿到同一失败；可重试失败允许后续请求重新触发，
 *       不可重试失败在有效期内直接复用；</li>
 *   <li>容量控制：执行并发受信号量限制，超出给出可区分拒绝；等待者数量有上限；</li>
 *   <li>等待与取消：等待超时/中断只放弃本次等待，不影响在途执行；</li>
 *   <li>有效期与清理：完成/失败/拒绝条目按 TTL 过期，过期后同键可重新执行，定时清理保证内存不无限增长。</li>
 * </ul>
 */
public class IdempotencyGateway {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGateway.class);

    private final IdempotencyProperties properties;
    private final FailureClassifier failureClassifier;

    private final ConcurrentHashMap<IdempotencyKey, IdempotencyEntry> entries = new ConcurrentHashMap<>();

    /** 执行并发上限（领导者获取，等待者不占用） */
    private final Semaphore executionSlots;

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "idempotency-cleaner");
        t.setDaemon(true);
        return t;
    });

    public IdempotencyGateway(IdempotencyProperties properties, FailureClassifier failureClassifier) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.failureClassifier = Objects.requireNonNull(failureClassifier, "failureClassifier");
        this.executionSlots = new Semaphore(properties.getMaxConcurrentExecutions(), true);
    }

    /**
     * 幂等执行：同一调用方内同一键在有效期内只触发一次下游副作用；
     * 在途请求并入同一次执行并复用结果。
     *
     * @throws IdempotencyException 失败原因见 {@link IdempotencyException.Reason}
     */
    public <T> T execute(String callerId, String key, Supplier<T> action) {
        IdempotencyKey scopedKey = new IdempotencyKey(
                Objects.requireNonNull(callerId, "callerId"),
                Objects.requireNonNull(key, "key"));

        JoinOutcome outcome = join(scopedKey);

        if (outcome.leader) {
            return runAsLeader(scopedKey, outcome.entry, action);
        }
        return waitAsFollower(outcome.entry, outcome.inFlightWaiter);
    }

    /**
     * 加入或创建键条目。在 ConcurrentHashMap.compute 内完成原子判断：
     * 新建者为领导者；在途键的后续请求为等待者；已完成/已失败条目直接复用。
     */
    private JoinOutcome join(IdempotencyKey scopedKey) {
        JoinOutcome outcome = new JoinOutcome();
        entries.compute(scopedKey, (k, existing) -> {
            long now = System.currentTimeMillis();
            if (existing != null && existing.isExpired(now)) {
                existing = null; // 已过期：允许重新执行
            }
            if (existing == null) {
                IdempotencyEntry created = new IdempotencyEntry(k);
                outcome.entry = created;
                outcome.leader = true;
                return created;
            }
            outcome.entry = existing;
            switch (existing.getState()) {
                case IN_PROGRESS:
                    if (existing.getWaiters() >= properties.getMaxWaiters()) {
                        throw new IdempotencyException(
                                IdempotencyException.Reason.REJECTED_TOO_MANY_WAITERS,
                                "在途键 " + k + " 的等待者数量已达上限 " + properties.getMaxWaiters());
                    }
                    existing.incrementWaiters();
                    outcome.inFlightWaiter = true;
                    return existing;
                case COMPLETED:
                case FAILED_NON_RETRYABLE:
                    // 有效期内直接复用缓存的结果 / 失败
                    return existing;
                case FAILED_RETRYABLE:
                case REJECTED:
                    // 可重试失败 / 拒绝状态仅保留用于观测可见性：
                    // 后续请求始终允许立即重新触发下游，不受保留期阻塞
                    IdempotencyEntry recreated = new IdempotencyEntry(k);
                    outcome.entry = recreated;
                    outcome.leader = true;
                    return recreated;
            }
            return existing;
        });
        return outcome;
    }

    private <T> T runAsLeader(IdempotencyKey scopedKey, IdempotencyEntry entry, Supplier<T> action) {
        if (!executionSlots.tryAcquire()) {
            IdempotencyException rejection = new IdempotencyException(
                    IdempotencyException.Reason.REJECTED_CAPACITY,
                    "执行并发已达上限 " + properties.getMaxConcurrentExecutions() + "，键 " + scopedKey + " 本次被拒绝");
            log.debug("容量拒绝: {}", scopedKey);
            entry.reject(rejection, properties.getRetryableFailureTtl());
            throw rejection;
        }
        try {
            T result = action.get();
            entry.complete(result, properties.getTtl());
            log.debug("执行完成: {}", scopedKey);
            return result;
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                // 恢复领导者线程的中断标志
                Thread.currentThread().interrupt();
            }
            boolean retryable = failureClassifier.isRetryable(t);
            entry.fail(t, retryable, retryable ? properties.getRetryableFailureTtl() : properties.getTtl());
            log.debug("执行失败({}): {}", retryable ? "可重试" : "不可重试", scopedKey, t);
            IdempotencyException.Reason reason = retryable
                    ? IdempotencyException.Reason.EXECUTION_FAILED_RETRYABLE
                    : IdempotencyException.Reason.EXECUTION_FAILED_NON_RETRYABLE;
            throw new IdempotencyException(reason, "下游执行失败，键 " + scopedKey, t);
        } finally {
            executionSlots.release();
        }
    }

    private <T> T waitAsFollower(IdempotencyEntry entry, boolean inFlightWaiter) {
        try {
            Object result = entry.getFuture().get(
                    properties.getWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            @SuppressWarnings("unchecked")
            T typed = (T) result;
            return typed;
        } catch (TimeoutException e) {
            // 仅放弃本次等待：不调用 future.cancel，在途执行不受影响
            throw new IdempotencyException(
                    IdempotencyException.Reason.WAIT_TIMEOUT,
                    "等待在途执行超时（" + properties.getWaitTimeout().toMillis() + "ms）: " + entry.getKey(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IdempotencyException(
                    IdempotencyException.Reason.INTERRUPTED, "等待被中断: " + entry.getKey(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IdempotencyException ie) {
                throw ie;
            }
            // 执行失败：所有等待方拿到与状态一致的同一失败结果
            boolean retryable = entry.getState() == EntryState.FAILED_RETRYABLE;
            IdempotencyException.Reason reason = retryable
                    ? IdempotencyException.Reason.EXECUTION_FAILED_RETRYABLE
                    : IdempotencyException.Reason.EXECUTION_FAILED_NON_RETRYABLE;
            throw new IdempotencyException(reason, "下游执行失败: " + entry.getKey(), cause);
        } finally {
            if (inFlightWaiter) {
                entry.decrementWaiters();
            }
        }
    }

    /** 查看指定键的当前状态（已过期条目视为不存在） */
    public Optional<KeyView> viewState(String callerId, String key) {
        IdempotencyEntry entry = entries.get(new IdempotencyKey(callerId, key));
        if (entry == null || entry.isExpired(System.currentTimeMillis())) {
            return Optional.empty();
        }
        return Optional.of(toView(entry));
    }

    /** 查看全部键状态快照（不含已过期条目） */
    public List<KeyView> listStates() {
        long now = System.currentTimeMillis();
        List<KeyView> views = new ArrayList<>();
        for (IdempotencyEntry entry : entries.values()) {
            if (!entry.isExpired(now)) {
                views.add(toView(entry));
            }
        }
        return views;
    }

    /** 清理指定键的状态；在途执行不受影响，其结果仍会正常结束 */
    public boolean clear(String callerId, String key) {
        return entries.remove(new IdempotencyKey(callerId, key)) != null;
    }

    private KeyView toView(IdempotencyEntry e) {
        return new KeyView(
                e.getKey(),
                e.getState(),
                e.getResult(),
                e.getError() == null ? null : e.getError().toString(),
                e.getWaiters(),
                e.getStartedAt(),
                e.getCompletedAt(),
                e.getExpiresAt());
    }

    @PostConstruct
    public void start() {
        long intervalMillis = properties.getCleanupInterval().toMillis();
        cleaner.scheduleAtFixedRate(this::cleanExpired, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("IdempotencyGateway 启动: ttl={}, waitTimeout={}, maxConcurrentExecutions={}, maxWaiters={}, cleanupInterval={}",
                properties.getTtl(), properties.getWaitTimeout(),
                properties.getMaxConcurrentExecutions(), properties.getMaxWaiters(),
                properties.getCleanupInterval());
    }

    @PreDestroy
    public void shutdown() {
        cleaner.shutdownNow();
        log.info("IdempotencyGateway 已关闭");
    }

    /** 清理已过有效期的终态条目；在途（IN_PROGRESS）条目不清理 */
    private void cleanExpired() {
        long now = System.currentTimeMillis();
        int before = entries.size();
        entries.entrySet().removeIf(e -> {
            IdempotencyEntry entry = e.getValue();
            return entry.getState() != EntryState.IN_PROGRESS && entry.isExpired(now);
        });
        int removed = before - entries.size();
        if (removed > 0) {
            log.debug("过期清理: 移除 {} 个条目", removed);
        }
    }

    /** 加入结果：是否为领导者、是否为在途等待者 */
    private static final class JoinOutcome {
        private IdempotencyEntry entry;
        private boolean leader;
        private boolean inFlightWaiter;
    }
}
