package com.github.diudiuseven.idempotentwritegatewayjava.config;

import com.github.diudiuseven.idempotentwritegatewayjava.core.FailureClassifier;
import com.github.diudiuseven.idempotentwritegatewayjava.core.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.core.NonRetryableException;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 幂等网关装配。
 */
@Configuration
@EnableConfigurationProperties(IdempotencyProperties.class)
public class GatewayConfiguration {

    /**
     * 默认失败分类：{@link NonRetryableException} 与 JVM 级错误（{@link Error}）不可重试，
     * 其余异常视为可重试。
     */
    @Bean
    public FailureClassifier failureClassifier() {
        return error -> !(error instanceof NonRetryableException) && !(error instanceof Error);
    }

    @Bean
    public IdempotencyGateway idempotencyGateway(IdempotencyProperties properties,
                                                  FailureClassifier failureClassifier) {
        return new IdempotencyGateway(properties, failureClassifier);
    }

    @Bean
    public WriteService writeService(IdempotencyProperties properties) {
        return new WriteService(properties.getDownstreamDelay());
    }
}
