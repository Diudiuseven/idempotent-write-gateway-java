package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.KeyStatus;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 写入接口：通过幂等网关收敛客户端重试与并发重复提交。
 */
@RestController
@RequestMapping("/api/writes")
public class WriteController {

    private final IdempotencyGateway gateway;
    private final DownstreamWriteService downstream;

    public WriteController(IdempotencyGateway gateway, DownstreamWriteService downstream) {
        this.gateway = gateway;
        this.downstream = downstream;
    }

    @PostMapping
    public WriteResponse write(@RequestBody WriteRequest request) {
        String output = gateway.execute(request.caller(), request.key(), () -> downstream.apply(request));
        return new WriteResponse(
                request.caller(),
                request.key(),
                KeyStatus.COMPLETED,
                output,
                downstream.invocationCount());
    }
}
