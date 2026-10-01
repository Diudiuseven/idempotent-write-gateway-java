package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

import java.util.concurrent.atomic.AtomicInteger;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.RetryableFailure;

import org.springframework.stereotype.Component;

/**
 * 模拟下游副作用服务：统计实际被触发的次数，用于验证副作用只发生一次。
 */
@Component
public class DownstreamWriteService {

    /** 可重试的下游失败（如临时网络错误），对应键会被立即释放。 */
    public static class TransientDownstreamException extends RuntimeException implements RetryableFailure {
        public TransientDownstreamException(String message) {
            super(message);
        }
    }

    /** 不可重试的下游失败（如业务校验失败），有效期内不会被反复重放。 */
    public static class FatalDownstreamException extends RuntimeException {
        public FatalDownstreamException(String message) {
            super(message);
        }
    }

    private final AtomicInteger invocations = new AtomicInteger();

    /**
     * 执行一次下游写入副作用，返回下游输出。
     */
    public String apply(WriteRequest request) {
        invocations.incrementAndGet();
        if (request.delayMillis() > 0) {
            try {
                Thread.sleep(request.delayMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransientDownstreamException("interrupted downstream call");
            }
        }
        String failMode = request.failMode() == null ? "NONE" : request.failMode();
        switch (failMode) {
            case "RETRYABLE" -> throw new TransientDownstreamException("transient downstream failure");
            case "FATAL" -> throw new FatalDownstreamException("fatal downstream failure");
            default -> {
                return "PROCESSED:" + request.payload();
            }
        }
    }

    /** 下游累计被调用次数。 */
    public int invocationCount() {
        return invocations.get();
    }
}
