package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

/**
 * 请求键的生命周期状态。
 */
public enum KeyStatus {
    /** 正在执行中，后到请求将合并等待。 */
    IN_FLIGHT,
    /** 执行成功，结果在有效期内被缓存。 */
    COMPLETED,
    /** 执行失败，失败结果在有效期内被缓存（仅不可重试失败）。 */
    FAILED,
    /** 因容量超限被拒绝（仅作为单次请求的结果状态，不落盘为键状态）。 */
    REJECTED
}
