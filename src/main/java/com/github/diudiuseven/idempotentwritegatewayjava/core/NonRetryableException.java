package com.github.diudiuseven.idempotentwritegatewayjava.core;

/**
 * 标记性异常：抛出该异常的执行失败被视为不可重试。
 */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
