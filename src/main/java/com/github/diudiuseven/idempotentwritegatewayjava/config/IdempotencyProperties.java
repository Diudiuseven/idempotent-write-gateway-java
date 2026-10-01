package com.github.diudiuseven.idempotentwritegatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 幂等网关配置项。
 */
@ConfigurationProperties(prefix = "idempotency")
public class IdempotencyProperties {

    /** 已完成/不可重试失败条目的有效期，过期后同一键可重新执行 */
    private Duration ttl = Duration.ofMinutes(10);

    /** 合并等待的最长等待时间，超时仅放弃本次等待，不影响在途执行 */
    private Duration waitTimeout = Duration.ofSeconds(30);

    /** 可重试失败/被拒绝条目的状态保留时长（仅影响观测可见性，不阻塞重试） */
    private Duration retryableFailureTtl = Duration.ofSeconds(1);

    /** 执行并发上限（同时进行的下游执行数） */
    private int maxConcurrentExecutions = 200;

    /** 单个在途键允许的最大等待者数量 */
    private int maxWaiters = 1000;

    /** 过期条目清理间隔 */
    private Duration cleanupInterval = Duration.ofSeconds(30);

    /** 下游写入模拟处理延迟（用于演示在途合并），默认 0 */
    private Duration downstreamDelay = Duration.ZERO;

    public Duration getTtl() {
        return ttl;
    }

    public void setTtl(Duration ttl) {
        this.ttl = ttl;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(Duration waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public Duration getRetryableFailureTtl() {
        return retryableFailureTtl;
    }

    public void setRetryableFailureTtl(Duration retryableFailureTtl) {
        this.retryableFailureTtl = retryableFailureTtl;
    }

    public int getMaxConcurrentExecutions() {
        return maxConcurrentExecutions;
    }

    public void setMaxConcurrentExecutions(int maxConcurrentExecutions) {
        this.maxConcurrentExecutions = maxConcurrentExecutions;
    }

    public int getMaxWaiters() {
        return maxWaiters;
    }

    public void setMaxWaiters(int maxWaiters) {
        this.maxWaiters = maxWaiters;
    }

    public Duration getCleanupInterval() {
        return cleanupInterval;
    }

    public void setCleanupInterval(Duration cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    public Duration getDownstreamDelay() {
        return downstreamDelay;
    }

    public void setDownstreamDelay(Duration downstreamDelay) {
        this.downstreamDelay = downstreamDelay;
    }
}
