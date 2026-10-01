package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

import java.util.Map;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.CapacityExceededException;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.WaitTimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 将幂等网关异常映射为可区分的 HTTP 结果：
 * 429 背压拒绝、504 等待超时、500 下游/业务失败。
 */
@RestControllerAdvice
public class GatewayExceptionHandler {

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, String>> capacity(CapacityExceededException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "CAPACITY_EXCEEDED", "message", String.valueOf(ex.getMessage())));
    }

    @ExceptionHandler(WaitTimeoutException.class)
    public ResponseEntity<Map<String, String>> waitTimeout(WaitTimeoutException ex) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(Map.of("error", "WAIT_TIMEOUT", "message", String.valueOf(ex.getMessage())));
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, String>> failure(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", ex.getClass().getSimpleName(), "message", String.valueOf(ex.getMessage())));
    }
}
