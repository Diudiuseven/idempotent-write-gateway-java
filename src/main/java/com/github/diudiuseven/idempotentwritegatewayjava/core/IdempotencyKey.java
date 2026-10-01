package com.github.diudiuseven.idempotentwritegatewayjava.core;

/**
 * 幂等键：按调用方隔离作用域。
 *
 * @param callerId 调用方标识
 * @param key      调用方内的业务键
 */
public record IdempotencyKey(String callerId, String key) {
}
