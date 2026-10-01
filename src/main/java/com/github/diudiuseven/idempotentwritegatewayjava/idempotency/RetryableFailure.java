package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 标记接口：实现该接口的异常被视为可重试失败，
 * 对应键会被立即释放，后续请求可重新触发执行。
 */
public interface RetryableFailure {
}
