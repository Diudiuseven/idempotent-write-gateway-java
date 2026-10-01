package com.github.diudiuseven.idempotentwritegatewayjava.downstream;

import java.time.Instant;

/**
 * 下游写入结果。
 */
public record WriteResult(String writeId, String echo, Instant processedAt) {
}
