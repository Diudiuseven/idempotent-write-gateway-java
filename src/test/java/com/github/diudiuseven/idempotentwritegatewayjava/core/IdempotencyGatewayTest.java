package com.github.diudiuseven.idempotentwritegatewayjava.core;

import com.github.diudiuseven.idempotentwritegatewayjava.config.IdempotencyProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等网关核心语义测试。
 */
class IdempotencyGatewayTest {

    private static final Duration AWAIT = Duration.ofSeconds(5);

    private final List<IdempotencyGateway> gateways = new ArrayList<>();
    private final List<ExecutorService> pools = new ArrayList<>();

    @AfterEach
    void tearDown() {
        pools.forEach(ExecutorService::shutdownNow);
        gateways.forEach(IdempotencyGateway::shutdown);
    }

    private IdempotencyProperties defaultProps() {
        IdempotencyProperties p = new IdempotencyProperties();
        p.setTtl(Duration.ofMinutes(10));
        p.setWaitTimeout(Duration.ofSeconds(5));
        p.setRetryableFailureTtl(Duration.ofSeconds(1));
        p.setMaxConcurrentExecutions(200);
        p.setMaxWaiters(1000);
        p.setCleanupInterval(Duration.ofSeconds(30));
        return p;
    }

    private IdempotencyGateway gateway(IdempotencyProperties props) {
        IdempotencyGateway g = new IdempotencyGateway(props, FailureClassifier.defaultClassifier());
        g.start();
        gateways.add(g);
        return g;
    }

    private ExecutorService pool() {
        ExecutorService p = Executors.newCachedThreadPool();
        pools.add(p);
        return p;
    }

    /** 轮询等待条件成立，避免引入额外测试依赖 */
    private void waitUntil(Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("等待条件超时");
    }

    private Supplier<String> blockingAction(AtomicInteger invocations, CountDownLatch started,
                                           CountDownLatch release, String result) {
        return () -> {
            invocations.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return result;
        };
    }

    // ============ 1. 并发重复提交：在途合并，副作用只发生一次 ============

    @Test
    void concurrentDuplicateSubscriptions_mergeIntoSingleExecution() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = blockingAction(invocations, started, release, "same-result");

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("callerA", "key1", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        int followers = 7;
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < followers; i++) {
            futures.add(pool.submit(() -> gw.execute("callerA", "key1", action)));
        }
        // 等待所有等待者就位
        waitUntil(() -> gw.viewState("callerA", "key1")
                .map(v -> v.state() == EntryState.IN_PROGRESS && v.waiters() == followers)
                .orElse(false));

        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("same-result");
        for (Future<String> f : futures) {
            assertThat(f.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("same-result");
        }
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 2. 作用域隔离：不同调用方互不串扰 ============

    @Test
    void differentCallers_sameKey_executeIndependently() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch startedA = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        CountDownLatch startedB = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();

        ExecutorService pool = pool();
        Future<String> fa = pool.submit(() -> gw.execute("callerA", "sameKey",
                blockingAction(invocations, startedA, releaseA, "A")));
        Future<String> fb = pool.submit(() -> gw.execute("callerB", "sameKey",
                blockingAction(invocations, startedB, releaseB, "B")));
        assertTrue(startedA.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(startedB.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        releaseA.countDown();
        releaseB.countDown();
        assertThat(fa.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("A");
        assertThat(fb.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("B");
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ============ 3. 同一调用方不同键互相独立 ============

    @Test
    void sameCaller_differentKeys_executeIndependently() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started1 = new CountDownLatch(1);
        CountDownLatch release1 = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();

        ExecutorService pool = pool();
        Future<String> f1 = pool.submit(() -> gw.execute("callerA", "key1",
                blockingAction(invocations, started1, release1, "one")));
        assertTrue(started1.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        // key2 不受 key1 在途影响，可立即执行
        String r2 = gw.execute("callerA", "key2", () -> {
            invocations.incrementAndGet();
            return "two";
        });
        assertThat(r2).isEqualTo("two");
        release1.countDown();
        assertThat(f1.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("one");
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ============ 4. 有效期内结果复用，不重复触发下游 ============

    @Test
    void completedResult_withinTtl_isReused() {
        IdempotencyGateway gw = gateway(defaultProps());
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = () -> {
            invocations.incrementAndGet();
            return "cached";
        };

        assertThat(gw.execute("c", "k", action)).isEqualTo("cached");
        assertThat(gw.execute("c", "k", action)).isEqualTo("cached");
        assertThat(gw.execute("c", "k", action)).isEqualTo("cached");
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 5. 可重试失败：后续请求允许重新触发 ============

    @Test
    void retryableFailure_allowsReExecution() {
        IdempotencyGateway gw = gateway(defaultProps());
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> failThenSucceed = new Supplier<>() {
            @Override
            public String get() {
                int n = invocations.incrementAndGet();
                if (n == 1) {
                    throw new RuntimeException("boom, retryable");
                }
                return "recovered";
            }
        };

        IdempotencyException ex = assertThrows(IdempotencyException.class,
                () -> gw.execute("c", "k", failThenSucceed));
        assertThat(ex.getReason()).isEqualTo(IdempotencyException.Reason.EXECUTION_FAILED_RETRYABLE);
        assertThat(ex.isRetryable()).isTrue();

        assertThat(gw.execute("c", "k", failThenSucceed)).isEqualTo("recovered");
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ============ 6. 不可重试失败：有效期内缓存，不重放 ============

    @Test
    void nonRetryableFailure_isCachedAndNotReplayed() {
        IdempotencyGateway gw = gateway(defaultProps());
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = () -> {
            invocations.incrementAndGet();
            throw new NonRetryableException("invalid request");
        };

        for (int i = 0; i < 3; i++) {
            IdempotencyException ex = assertThrows(IdempotencyException.class,
                    () -> gw.execute("c", "k", action));
            assertThat(ex.getReason()).isEqualTo(IdempotencyException.Reason.EXECUTION_FAILED_NON_RETRYABLE);
            assertThat(ex.isRetryable()).isFalse();
        }
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 7. 不可重试失败过期后可重新执行 ============

    @Test
    void nonRetryableFailure_expiresAfterTtl_andCanReExecute() throws Exception {
        IdempotencyProperties props = defaultProps();
        props.setTtl(Duration.ofMillis(150));
        IdempotencyGateway gw = gateway(props);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = () -> {
            int n = invocations.incrementAndGet();
            if (n == 1) {
                throw new NonRetryableException("invalid request");
            }
            return "after-ttl";
        };

        assertThrows(IdempotencyException.class, () -> gw.execute("c", "k", action));
        Thread.sleep(300);
        assertThat(gw.execute("c", "k", action)).isEqualTo("after-ttl");
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ============ 8. 等待超时：只放弃本次等待，不影响在途执行 ============

    @Test
    void waitTimeout_doesNotCancelInFlightExecution() throws Exception {
        IdempotencyProperties props = defaultProps();
        props.setWaitTimeout(Duration.ofMillis(200));
        IdempotencyGateway gw = gateway(props);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = blockingAction(invocations, started, release, "done");

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        IdempotencyException ex = assertThrows(IdempotencyException.class,
                () -> gw.execute("c", "k", action));
        assertThat(ex.getReason()).isEqualTo(IdempotencyException.Reason.WAIT_TIMEOUT);

        // 在途执行未被取消：释放后正常完成，后续请求复用结果
        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(gw.execute("c", "k", action)).isEqualTo("done");
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 9. 等待者被中断：不影响在途执行 ============

    @Test
    void waiterInterrupted_doesNotAffectInFlightExecution() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = blockingAction(invocations, started, release, "done");

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        // 在独立线程中等待，然后中断该等待线程
        java.util.concurrent.atomic.AtomicReference<IdempotencyException> caught =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread waiterThread = new Thread(() -> {
            try {
                gw.execute("c", "k", action);
            } catch (IdempotencyException e) {
                caught.set(e);
            }
        }, "test-waiter");
        waiterThread.start();
        waitUntil(() -> gw.viewState("c", "k")
                .map(v -> v.waiters() == 1)
                .orElse(false));
        waiterThread.interrupt();
        waiterThread.join(AWAIT.toMillis());
        assertThat(caught.get()).isNotNull();
        assertThat(caught.get().getReason()).isEqualTo(IdempotencyException.Reason.INTERRUPTED);

        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 10. 执行并发上限：超出给出可区分拒绝 ============

    @Test
    void capacityRejection_isDistinguishableAndRecoverable() throws Exception {
        IdempotencyProperties props = defaultProps();
        props.setMaxConcurrentExecutions(1);
        IdempotencyGateway gw = gateway(props);
        CountDownLatch startedA = new CountDownLatch(1);
        CountDownLatch releaseA = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();

        ExecutorService pool = pool();
        Future<String> fa = pool.submit(() -> gw.execute("c", "keyA",
                blockingAction(invocations, startedA, releaseA, "A")));
        assertTrue(startedA.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        // 不同键：容量不足 → 可区分拒绝
        IdempotencyException ex = assertThrows(IdempotencyException.class,
                () -> gw.execute("c", "keyB", () -> {
                    invocations.incrementAndGet();
                    return "B";
                }));
        assertThat(ex.getReason()).isEqualTo(IdempotencyException.Reason.REJECTED_CAPACITY);

        // 拒绝状态可被观测到
        Optional<KeyView> rejected = gw.viewState("c", "keyB");
        assertThat(rejected).isPresent();
        assertThat(rejected.get().state()).isEqualTo(EntryState.REJECTED);

        releaseA.countDown();
        assertThat(fa.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("A");

        // 容量恢复后被拒绝的键可重新执行
        assertThat(gw.execute("c", "keyB", () -> {
            invocations.incrementAndGet();
            return "B";
        })).isEqualTo("B");
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ============ 11. 等待者数量上限：超出给出可区分拒绝 ============

    @Test
    void tooManyWaiters_rejected() throws Exception {
        IdempotencyProperties props = defaultProps();
        props.setMaxWaiters(1);
        IdempotencyGateway gw = gateway(props);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = blockingAction(invocations, started, release, "done");

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        // 第一个等待者正常并入
        Future<String> waiter1 = pool.submit(() -> gw.execute("c", "k", action));
        waitUntil(() -> gw.viewState("c", "k")
                .map(v -> v.waiters() == 1)
                .orElse(false));

        // 第二个等待者超出上限 → 可区分拒绝
        IdempotencyException ex = assertThrows(IdempotencyException.class,
                () -> gw.execute("c", "k", action));
        assertThat(ex.getReason()).isEqualTo(IdempotencyException.Reason.REJECTED_TOO_MANY_WAITERS);

        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(waiter1.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 12. 一致失败：所有等待方拿到同一失败结果 ============

    @Test
    void allWaiters_getSameFailure() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        RuntimeException failure = new RuntimeException("downstream 500");
        Supplier<String> action = () -> {
            invocations.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            throw failure;
        };

        ExecutorService pool = pool();
        Future<?> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));
        List<Future<?>> waiterFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            waiterFutures.add(pool.submit(() -> {
                gw.execute("c", "k", action);
                return null;
            }));
        }
        waitUntil(() -> gw.viewState("c", "k")
                .map(v -> v.waiters() == 5)
                .orElse(false));

        release.countDown();
        assertThrows(Exception.class, () -> leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));
        for (Future<?> f : waiterFutures) {
            ExecutionExceptionAssertion.assertThatThrowsWithCause(f, failure);
        }
        assertThat(invocations.get()).isEqualTo(1);
    }

    // ============ 13. 可观测：执行中 / 已完成状态 ============

    @Test
    void observability_reportsInProgressAndCompletedStates() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Supplier<String> action = blockingAction(new AtomicInteger(), started, release, "ok");

        assertThat(gw.viewState("c", "k")).isEmpty();

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        KeyView inProgress = gw.viewState("c", "k").orElseThrow();
        assertThat(inProgress.state()).isEqualTo(EntryState.IN_PROGRESS);
        assertThat(inProgress.startedAt()).isNotNull();

        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("ok");

        KeyView completed = gw.viewState("c", "k").orElseThrow();
        assertThat(completed.state()).isEqualTo(EntryState.COMPLETED);
        assertThat(completed.result()).isEqualTo("ok");
        assertThat(completed.completedAt()).isNotNull();
        assertThat(completed.expiresAt()).isNotNull();
        assertThat(gw.listStates()).hasSize(1);
    }

    // ============ 14. 清理：clear 不影响在途执行 ============

    @Test
    void clear_doesNotAffectInFlightExecution() throws Exception {
        IdempotencyGateway gw = gateway(defaultProps());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Supplier<String> action = blockingAction(invocations, started, release, "done");

        ExecutorService pool = pool();
        Future<String> leader = pool.submit(() -> gw.execute("c", "k", action));
        assertTrue(started.await(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));

        assertThat(gw.clear("c", "k")).isTrue();
        assertThat(gw.viewState("c", "k")).isEmpty();

        release.countDown();
        assertThat(leader.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(invocations.get()).isEqualTo(1);
        // 在途执行结束后不会重新挂回状态表
        assertThat(gw.viewState("c", "k")).isEmpty();
    }

    // ============ 15. 清理：过期条目被定时任务移除，内存不无限增长 ============

    @Test
    void expiredEntries_areRemovedByCleaner() throws Exception {
        IdempotencyProperties props = defaultProps();
        props.setTtl(Duration.ofMillis(80));
        props.setCleanupInterval(Duration.ofMillis(50));
        IdempotencyGateway gw = gateway(props);

        gw.execute("c", "k1", () -> "r1");
        gw.execute("c", "k2", () -> "r2");
        assertThat(gw.listStates()).hasSize(2);

        waitUntil(() -> gw.listStates().isEmpty());
        assertThat(gw.viewState("c", "k1")).isEmpty();
        assertThat(gw.viewState("c", "k2")).isEmpty();

        // 过期后同键可重新执行
        assertThat(gw.execute("c", "k1", () -> "r1-new")).isEqualTo("r1-new");
    }

    /** 辅助断言：Future 抛出的 ExecutionException 因果链中包含指定失败 */
    private static final class ExecutionExceptionAssertion {
        static void assertThatThrowsWithCause(Future<?> future, Throwable expectedCause) {
            Throwable thrown = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> future.get(AWAIT.getSeconds(), java.util.concurrent.TimeUnit.SECONDS));
            boolean found = false;
            for (Throwable c = thrown; c != null; c = c.getCause()) {
                if (c instanceof IdempotencyException ie
                        && ie.getReason() == IdempotencyException.Reason.EXECUTION_FAILED_RETRYABLE
                        && c.getCause() == expectedCause) {
                    found = true;
                    break;
                }
            }
            assertThat(found).as("等待方应拿到与领导者一致的失败结果").isTrue();
        }
    }
}
