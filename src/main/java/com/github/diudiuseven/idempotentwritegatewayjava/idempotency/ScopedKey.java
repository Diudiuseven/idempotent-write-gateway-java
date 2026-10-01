package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

import java.util.Objects;

/**
 * 按调用方隔离的请求键。不同调用方的相同 key 互不影响。
 */
public record ScopedKey(String caller, String key) {

    public ScopedKey {
        Objects.requireNonNull(caller, "caller must not be null");
        Objects.requireNonNull(key, "key must not be null");
        if (caller.isBlank() || key.isBlank()) {
            throw new IllegalArgumentException("caller and key must not be blank");
        }
    }
}
