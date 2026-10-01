package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 幂等执行相关配置项。
 */
@ConfigurationProperties(prefix = "idempotency")
public class IdempotencyProperties {

    /** 已完成/失败结果在内存中的保留时长，过期后同一键可重新执行。 */
    private Duration entryTtl = Duration.ofMinutes(10);

    /** 合并等待方等待在途执行结果的最长时长。 */
    private Duration waitTimeout = Duration.ofSeconds(30);

    /** 过期条目清理任务的执行间隔。 */
    private Duration cleanupInterval = Duration.ofMinutes(1);

    /** 同时处于执行中的最大请求数，超出则拒绝。 */
    private int maxInFlight = 64;

    /** 同一个键允许挂起的最大等待方数量，超出则拒绝。 */
    private int maxWaitersPerKey = 256;

    /** 内存中保留的最大键数量，超出则拒绝新键。 */
    private int maxEntries = 10_000;

    public Duration getEntryTtl() {
        return entryTtl;
    }

    public void setEntryTtl(Duration entryTtl) {
        this.entryTtl = entryTtl;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(Duration waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public Duration getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(Duration cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public int getMaxInFlight() {
        return maxInFlight;
    }

    public void setMaxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
    }

    public int getMaxWaitersPerKey() {
        return maxWaitersPerKey;
    }

    public void setMaxWaitersPerKey(int maxWaitersPerKey) {
        this.maxWaitersPerKey = maxWaitersPerKey;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }
}
