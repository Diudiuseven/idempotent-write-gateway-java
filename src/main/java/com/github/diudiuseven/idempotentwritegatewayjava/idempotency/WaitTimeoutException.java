package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 合并等待方在配置的等待上限内未拿到结果时抛出。
 * 不影响正在进行的那次执行。
 */
public class WaitTimeoutException extends IdempotencyException {

    public WaitTimeoutException(String message) {
        super(message);
    }
}
