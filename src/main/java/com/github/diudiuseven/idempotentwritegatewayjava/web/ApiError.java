package com.github.diudiuseven.idempotentwritegatewayjava.web;

/**
 * 统一错误响应体。
 */
public record ApiError(String reason, boolean retryable, String message) {
}
