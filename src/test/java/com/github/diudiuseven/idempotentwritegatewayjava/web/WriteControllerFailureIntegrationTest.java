package com.github.diudiuseven.idempotentwritegatewayjava.web;

import com.github.diudiuseven.idempotentwritegatewayjava.core.NonRetryableException;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 失败语义 HTTP 映射测试：不可重试失败返回 422，可重试失败返回 500，
 * 错误体带 reason 与 retryable 标志。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WriteControllerFailureIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @MockitoBean
    private WriteService writeService;

    private HttpEntity<String> entity(String key) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(WriteController.HEADER_CALLER_ID, "callerA");
        headers.set(WriteController.HEADER_IDEMPOTENCY_KEY, key);
        return new HttpEntity<>("payload", headers);
    }

    @Test
    void nonRetryableFailure_returns422WithReason() {
        when(writeService.process(anyString(), anyString()))
                .thenThrow(new NonRetryableException("业务校验失败，不可重试"));

        ResponseEntity<ApiError> resp = rest.postForEntity(
                "/api/writes", entity("fail-non-retryable"), ApiError.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().reason()).isEqualTo("EXECUTION_FAILED_NON_RETRYABLE");
        assertThat(resp.getBody().retryable()).isFalse();
    }

    @Test
    void retryableFailure_returns500AndRetryableFlag() {
        when(writeService.process(anyString(), anyString()))
                .thenThrow(new RuntimeException("downstream 500"));

        ResponseEntity<ApiError> resp = rest.postForEntity(
                "/api/writes", entity("fail-retryable"), ApiError.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().reason()).isEqualTo("EXECUTION_FAILED_RETRYABLE");
        assertThat(resp.getBody().retryable()).isTrue();
    }
}
