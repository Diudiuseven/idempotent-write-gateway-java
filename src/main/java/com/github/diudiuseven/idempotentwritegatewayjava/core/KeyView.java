package com.github.diudiuseven.idempotentwritegatewayjava.core;

import java.time.Instant;

/**
 * 幂等条目快照，用于运维查看。
 */
public record KeyView(
        IdempotencyKey key,
        EntryState state,
        Object result,
        String error,
        int waiters,
        Instant startedAt,
        Instant completedAt,
        Instant expiresAt) {
}
