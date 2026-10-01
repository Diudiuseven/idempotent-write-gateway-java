package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 失败分类策略：判断一次执行失败是否允许后续请求重试。
 */
@FunctionalInterface
public interface FailurePolicy {

    boolean isRetryable(Throwable failure);
}
