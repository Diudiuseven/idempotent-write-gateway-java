package com.github.diudiuseven.idempotentwritegatewayjava.core;

/**
 * 幂等条目的生命周期状态。
 */
public enum EntryState {
    /** 执行中（在途），后续请求应并入同一次执行 */
    IN_PROGRESS,
    /** 已完成，结果在有效期内可直接复用 */
    COMPLETED,
    /** 执行失败且可重试，后续请求允许重新触发下游 */
    FAILED_RETRYABLE,
    /** 执行失败且不可重试，有效期内直接复用失败结果，不重放 */
    FAILED_NON_RETRYABLE,
    /** 因容量不足（执行并发/等待者超限）被拒绝 */
    REJECTED
}
