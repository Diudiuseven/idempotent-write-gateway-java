package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.KeyStatus;

/**
 * 写入响应：包含幂等执行结果状态与下游实际被调用次数（用于验证副作用只发生一次）。
 */
public record WriteResponse(
        String caller,
        String key,
        KeyStatus status,
        String output,
        int downstreamInvocations) {
}
