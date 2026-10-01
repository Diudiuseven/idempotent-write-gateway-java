package com.github.diudiuseven.idempotentwritegatewayjava.downstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 演示用下游写入服务：代表真正产生副作用的系统（如扣款、下单）。
 *
 * <p>通过 {@link #invocationCount()} 统计实际被调用次数，用于验证幂等效果。
 */
public class WriteService {

    private static final Logger log = LoggerFactory.getLogger(WriteService.class);

    private final AtomicLong invocations = new AtomicLong();
    private final long delayMillis;

    public WriteService() {
        this(Duration.ZERO);
    }

    public WriteService(Duration delay) {
        this.delayMillis = delay == null ? 0L : delay.toMillis();
    }

    public WriteResult process(String idempotencyKey, String payload) {
        long n = invocations.incrementAndGet();
        log.info("下游写入被调用: key={}, payload={}, 第 {} 次", idempotencyKey, payload, n);
        if (delayMillis > 0) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("下游处理被中断", e);
            }
        }
        return new WriteResult(UUID.randomUUID().toString(), payload, Instant.now());
    }

    /** 下游实际被调用次数（用于测试验证副作用只发生一次） */
    public long invocationCount() {
        return invocations.get();
    }
}
