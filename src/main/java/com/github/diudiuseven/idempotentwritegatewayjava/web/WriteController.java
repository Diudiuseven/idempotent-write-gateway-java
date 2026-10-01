package com.github.diudiuseven.idempotentwritegatewayjava.web;

import com.github.diudiuseven.idempotentwritegatewayjava.core.IdempotencyException;
import com.github.diudiuseven.idempotentwritegatewayjava.core.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteResult;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 写入接口：通过 {@value #HEADER_IDEMPOTENCY_KEY} 与 {@value #HEADER_CALLER_ID}
 * 提供服务端幂等执行与在途请求合并。
 */
@RestController
@RequestMapping("/api/writes")
public class WriteController {

    public static final String HEADER_IDEMPOTENCY_KEY = "Idempotency-Key";
    public static final String HEADER_CALLER_ID = "X-Caller-Id";

    private final IdempotencyGateway gateway;
    private final WriteService writeService;

    public WriteController(IdempotencyGateway gateway, WriteService writeService) {
        this.gateway = gateway;
        this.writeService = writeService;
    }

    @PostMapping
    public ResponseEntity<WriteResult> write(
            @RequestHeader(HEADER_CALLER_ID) String callerId,
            @RequestHeader(HEADER_IDEMPOTENCY_KEY) String idempotencyKey,
            @RequestBody(required = false) String payload) {
        WriteResult result = gateway.execute(callerId, idempotencyKey,
                () -> writeService.process(idempotencyKey, payload));
        return ResponseEntity.ok(result);
    }

    @ExceptionHandler(IdempotencyException.class)
    public ResponseEntity<ApiError> handle(IdempotencyException e) {
        HttpStatus status = switch (e.getReason()) {
            case WAIT_TIMEOUT -> HttpStatus.REQUEST_TIMEOUT;
            case REJECTED_CAPACITY, REJECTED_TOO_MANY_WAITERS -> HttpStatus.SERVICE_UNAVAILABLE;
            case EXECUTION_FAILED_NON_RETRYABLE -> HttpStatus.UNPROCESSABLE_ENTITY;
            case EXECUTION_FAILED_RETRYABLE, INTERRUPTED -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
        return ResponseEntity.status(status)
                .body(new ApiError(e.getReason().name(), e.isRetryable(), e.getMessage()));
    }
}
