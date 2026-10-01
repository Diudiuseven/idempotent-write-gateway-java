package com.github.diudiuseven.idempotentwritegatewayjava.core;

/**
 * 幂等网关统一异常。
 */
public class IdempotencyException extends RuntimeException {

    public enum Reason {
        /** 执行失败（可重试），后续请求可重新触发下游 */
        EXECUTION_FAILED_RETRYABLE,
        /** 执行失败（不可重试），有效期内直接复用失败结果 */
        EXECUTION_FAILED_NON_RETRYABLE,
        /** 等待在途执行超时（本次等待超时，不影响在途执行） */
        WAIT_TIMEOUT,
        /** 执行并发已满，本次请求被拒绝 */
        REJECTED_CAPACITY,
        /** 等待者数量已达上限，本次请求被拒绝 */
        REJECTED_TOO_MANY_WAITERS,
        /** 等待线程被中断 */
        INTERRUPTED
    }

    private final Reason reason;

    public IdempotencyException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public IdempotencyException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }

    /** 该失败是否允许后续请求重试 */
    public boolean isRetryable() {
        return reason == Reason.EXECUTION_FAILED_RETRYABLE;
    }
}
