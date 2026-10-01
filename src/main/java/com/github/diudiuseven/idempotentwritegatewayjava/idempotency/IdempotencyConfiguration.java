package com.github.diudiuseven.idempotentwritegatewayjava.idempotency;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 幂等网关的自动装配。
 */
@Configuration
@EnableConfigurationProperties(IdempotencyProperties.class)
public class IdempotencyConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public FailurePolicy failurePolicy() {
        return failure -> failure instanceof RetryableFailure;
    }

    @Bean
    public IdempotencyGateway idempotencyGateway(IdempotencyProperties properties, FailurePolicy failurePolicy) {
        return new IdempotencyGateway(properties, failurePolicy);
    }
}
