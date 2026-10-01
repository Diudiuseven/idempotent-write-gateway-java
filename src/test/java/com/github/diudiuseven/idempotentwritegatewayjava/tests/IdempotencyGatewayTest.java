package com.github.diudiuseven.idempotentwritegatewayjava.tests;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.CapacityExceededException;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.FailurePolicy;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.IdempotencyProperties;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.KeyStateView;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.KeyStatus;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.RetryableFailure;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.WaitTimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyGatewayTest {

    private static final FailurePolicy DEFAULT_POLICY = failure -> failure instanceof RetryableFailure;

    private final MutableClock clock = new MutableClock();
    private IdempotencyGateway gateway;

    @AfterEach
    void tearDown() {
        if (gateway != null) {
            gateway.close();
        }
    }

    private IdempotencyGateway newGateway(java.util.function.Consumer<IdempotencyProperties> tuner) {
        IdempotencyProperties props = new IdempotencyProperties();
        props.setCleanupInterval(Duration.ofMillis(50));
        tuner.accept(props);
        gateway = new IdempotencyGateway(props, DEFAULT_POLICY, clock);
        return gateway;
    }

    @Test
    void concurrentDuplicateSubmissionsTriggerSingleExecution() throws Exception {
        IdempotencyGateway gw = newGateway(p -> {
        });
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> gw.execute("caller-a", "k1", () -> {
                executions.incrementAndGet();
                entered.countDown();
                await(release);
                return "result-1";
            })));
        }
        // 等待执行者进入后再放行，确保其余请求都合并到这一次执行
        await(entered);
        release.countDown();

        for (Future<String> f : futures) {
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("result-1");
        }
        assertThat(executions.get()).isEqualTo(1);
        pool.shutdownNow();
    }

    @Test
    void keysAreIsolatedPerCaller() {
        IdempotencyGateway gw = newGateway(p -> {
        });
        AtomicInteger executions = new AtomicInteger();

        String r1 = gw.execute("caller-a", "same-key", () -> "a-" + executions.incrementAndGet());
        String r2 = gw.execute("caller-b", "same-key", () -> "b-" + executions.incrementAndGet());
        String r3 = gw.execute("caller-a", "same-key", () -> "a-" + executions.incrementAndGet());

        assertThat(r1).isEqualTo("a-1");
        assertThat(r2).isEqualTo("b-2");
        assertThat(r3).isEqualTo("a-1"); // 同一调用方命中缓存
        assertThat(executions.get()).isEqualTo(2);
    }

    @Test
    void failureIsPropagatedConsistentlyToAllWaiters() throws Exception {
        IdempotencyGateway gw = newGateway(p -> {
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> gw.execute("caller-a", "boom", () -> {
                entered.countDown();
                await(release);
                throw new IllegalStateException("consistent failure");
            })));
        }
        await(entered);
        release.countDown();

        for (Future<String> f : futures) {
            assertThatThrownBy(f::get)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .rootCause()
                    .hasMessage("consistent failure");
        }
        pool.shutdownNow();
    }

    @Test
    void retryableFailureAllowsSubsequentRetry() {
        IdempotencyGateway gw = newGateway(p -> {
        });
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> gw.execute("caller-a", "k", () -> {
            attempts.incrementAndGet();
            throw new TransientFailure();
        })).isInstanceOf(TransientFailure.class);

        // 可重试失败：键已释放，后续请求重新触发
        String result = gw.execute("caller-a", "k", () -> "ok-" + attempts.incrementAndGet());
        assertThat(result).isEqualTo("ok-2");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void nonRetryableFailureIsNotReplayedWithinTtl() {
        IdempotencyGateway gw = newGateway(p -> {
        });
        AtomicInteger executions = new AtomicInteger();

        assertThatThrownBy(() -> gw.execute("caller-a", "k", () -> {
            executions.incrementAndGet();
            throw new IllegalStateException("fatal");
        })).isInstanceOf(IllegalStateException.class);

        // 有效期内：直接重放失败，不再触发执行
        assertThatThrownBy(() -> gw.execute("caller-a", "k", () -> {
            executions.incrementAndGet();
            return "should-not-run";
        })).isInstanceOf(IllegalStateException.class).hasMessage("fatal");
        assertThat(executions.get()).isEqualTo(1);

        // 过期后：允许重新执行
        clock.advance(Duration.ofMinutes(11));
        String result = gw.execute("caller-a", "k", () -> "recovered-" + executions.incrementAndGet());
        assertThat(result).isEqualTo("recovered-2");
    }

    @Test
    void waiterTimesOutWithoutAffectingExecution() throws Exception {
        IdempotencyGateway gw = newGateway(p -> p.setWaitTimeout(Duration.ofMillis(100)));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<String> leader = pool.submit(() -> gw.execute("caller-a", "slow", () -> {
            executions.incrementAndGet();
            entered.countDown();
            await(release);
            return "slow-result";
        }));
        await(entered);

        // 等待方超时退出
        assertThatThrownBy(() -> gw.execute("caller-a", "slow", () -> "never"))
                .isInstanceOf(WaitTimeoutException.class);

        // 在途执行不受影响，仍能完成并缓存结果
        release.countDown();
        assertThat(leader.get(5, TimeUnit.SECONDS)).isEqualTo("slow-result");
        assertThat(gw.execute("caller-a", "slow", () -> "never")).isEqualTo("slow-result");
        assertThat(executions.get()).isEqualTo(1);
        pool.shutdownNow();
    }

    @Test
    void expiredEntryCanBeReexecutedAndCleanedUp() {
        IdempotencyGateway gw = newGateway(p -> p.setEntryTtl(Duration.ofMinutes(1)));
        AtomicInteger executions = new AtomicInteger();

        assertThat(gw.execute("caller-a", "k", () -> "v" + executions.incrementAndGet())).isEqualTo("v1");
        assertThat(gw.list()).hasSize(1);

        clock.advance(Duration.ofMinutes(2));
        // 过期后同一键重新执行
        assertThat(gw.execute("caller-a", "k", () -> "v" + executions.incrementAndGet())).isEqualTo("v2");
        assertThat(executions.get()).isEqualTo(2);

        // 清理任务最终移除过期条目（cleanupInterval=50ms）
        clock.advance(Duration.ofMinutes(2));
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> gw.list().isEmpty());
    }

    @Test
    void inFlightLimitRejectsWithDistinguishableResult() throws Exception {
        IdempotencyGateway gw = newGateway(p -> p.setMaxInFlight(1));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<String> first = pool.submit(() -> gw.execute("caller-a", "k1", () -> {
            entered.countDown();
            await(release);
            return "done";
        }));
        await(entered);

        // 另一个键的在途执行超出上限，被拒绝
        assertThatThrownBy(() -> gw.execute("caller-a", "k2", () -> "never"))
                .isInstanceOf(CapacityExceededException.class);

        release.countDown();
        assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("done");
        pool.shutdownNow();
    }

    @Test
    void waiterLimitRejectsExcessWaiters() throws Exception {
        IdempotencyGateway gw = newGateway(p -> {
            p.setMaxWaitersPerKey(1);
            p.setWaitTimeout(Duration.ofSeconds(5));
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<String> leader = pool.submit(() -> gw.execute("caller-a", "k", () -> {
            entered.countDown();
            await(release);
            return "done";
        }));
        await(entered);
        Future<String> waiter = pool.submit(() -> gw.execute("caller-a", "k", () -> "never"));
        // 等 waiter 挂上后再验证超限拒绝
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> gw.inspect("caller-a", "k").map(KeyStateView::waiterCount).orElse(0) == 1);

        assertThatThrownBy(() -> gw.execute("caller-a", "k", () -> "never"))
                .isInstanceOf(CapacityExceededException.class);

        release.countDown();
        assertThat(leader.get(5, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(waiter.get(5, TimeUnit.SECONDS)).isEqualTo("done");
        pool.shutdownNow();
    }

    @Test
    void inspectListAndEvictExposeOperationalState() throws Exception {
        IdempotencyGateway gw = newGateway(p -> {
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<String> inFlight = pool.submit(() -> gw.execute("caller-a", "running", () -> {
            entered.countDown();
            await(release);
            return "done";
        }));
        await(entered);
        assertThat(gw.inspect("caller-a", "running")).get()
                .extracting(KeyStateView::status).isEqualTo(KeyStatus.IN_FLIGHT);

        release.countDown();
        assertThat(inFlight.get(5, TimeUnit.SECONDS)).isEqualTo("done");
        assertThat(gw.inspect("caller-a", "running")).get()
                .extracting(KeyStateView::status).isEqualTo(KeyStatus.COMPLETED);

        assertThatThrownBy(() -> gw.execute("caller-a", "failed", () -> {
            throw new IllegalStateException("fatal");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(gw.inspect("caller-a", "failed")).get()
                .extracting(KeyStateView::status).isEqualTo(KeyStatus.FAILED);
        assertThat(gw.list()).hasSize(2);
        pool.shutdownNow();
    }

    @Test
    void evictRemovesKeyState() {
        IdempotencyGateway gw = newGateway(p -> {
        });
        AtomicInteger executions = new AtomicInteger();
        gw.execute("caller-a", "k", () -> "v" + executions.incrementAndGet());
        assertThat(gw.list()).hasSize(1);

        assertThat(gw.evict("caller-a", "k")).isTrue();
        assertThat(gw.inspect("caller-a", "k")).isEmpty();
        assertThat(gw.evict("caller-a", "k")).isFalse();

        // 清理后可重新执行
        assertThat(gw.execute("caller-a", "k", () -> "v" + executions.incrementAndGet())).isEqualTo("v2");
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class TransientFailure extends RuntimeException implements RetryableFailure {
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.now();

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
