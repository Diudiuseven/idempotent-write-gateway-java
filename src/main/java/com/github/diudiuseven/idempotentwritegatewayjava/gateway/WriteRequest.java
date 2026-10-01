package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

/**
 * 写入请求演示载荷。failMode 用于演示/测试下游失败：NONE / RETRYABLE / FATAL。
 */
public record WriteRequest(
        String caller,
        String key,
        String payload,
        long delayMillis,
        String failMode) {
}
