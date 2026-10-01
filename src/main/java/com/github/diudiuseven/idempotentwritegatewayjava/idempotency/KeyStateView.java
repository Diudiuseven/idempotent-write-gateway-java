package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

import java.time.Instant;

/**
 * 键当前状态的只读视图，用于可观测与运维接口。
 */
public record KeyStateView(
        String caller,
        String key,
        KeyStatus status,
        Instant createdAt,
        Instant expiresAt,
        int waiterCount,
        String failureMessage,
        boolean retryableFailure) {
}
