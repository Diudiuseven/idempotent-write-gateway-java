package com.github.diudiuseven.idempotentwritegatewayjava.core;

/**
 * 失败分类器：判断一次执行失败是否可重试。
 */
@FunctionalInterface
public interface FailureClassifier {

    /**
     * @param error 执行抛出的异常
     * @return true 表示可重试（后续请求可重新触发下游）；false 表示不可重试（有效期内缓存失败结果）
     */
    boolean isRetryable(Throwable error);

    /**
     * 默认分类：{@link NonRetryableException} 与 JVM 级错误（{@link Error}）不可重试，
     * 其余异常视为可重试。
     */
    static FailureClassifier defaultClassifier() {
        return error -> !(error instanceof NonRetryableException) && !(error instanceof Error);
    }
}
