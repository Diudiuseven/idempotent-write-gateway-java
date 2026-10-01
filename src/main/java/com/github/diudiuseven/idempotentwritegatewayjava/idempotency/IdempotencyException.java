package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 幂等网关自身抛出的异常基类（区别于下游动作抛出的业务异常）。
 */
public class IdempotencyException extends RuntimeException {

    public IdempotencyException(String message) {
        super(message);
    }

    public IdempotencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
