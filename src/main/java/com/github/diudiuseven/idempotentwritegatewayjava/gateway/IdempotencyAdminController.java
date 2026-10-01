package com.github.diudiuseven.idempotentwritegatewayjava.gateway;

import java.util.List;

import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.idempotency.KeyStateView;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 幂等状态的运维接口：查看与主动清理键状态。
 */
@RestController
@RequestMapping("/api/idempotency/keys")
public class IdempotencyAdminController {

    private final IdempotencyGateway gateway;

    public IdempotencyAdminController(IdempotencyGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping
    public List<KeyStateView> list() {
        return gateway.list();
    }

    @GetMapping("/{caller}/{key}")
    public ResponseEntity<KeyStateView> inspect(@PathVariable String caller, @PathVariable String key) {
        return gateway.inspect(caller, key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{caller}/{key}")
    public ResponseEntity<Void> evict(@PathVariable String caller, @PathVariable String key) {
        return gateway.evict(caller, key)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }
}
