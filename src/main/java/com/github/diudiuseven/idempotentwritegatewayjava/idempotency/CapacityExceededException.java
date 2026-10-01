package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 并发度或容量超限时的拒绝结果，调用方可据此区分背压与业务失败。
 */
public class CapacityExceededException extends IdempotencyException {

    public CapacityExceededException(String message) {
        super(message);
    }
}
