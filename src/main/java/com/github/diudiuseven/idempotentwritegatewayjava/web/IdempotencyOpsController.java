package com.github.diudiuseven.idempotentwritegatewayjava.web;

import com.github.diudiuseven.idempotentwritegatewayjava.core.IdempotencyGateway;
import com.github.diudiuseven.idempotentwritegatewayjava.core.KeyView;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 运维接口：查看/清理幂等键状态。
 */
@RestController
@RequestMapping("/api/idempotency")
public class IdempotencyOpsController {

    private final IdempotencyGateway gateway;

    public IdempotencyOpsController(IdempotencyGateway gateway) {
        this.gateway = gateway;
    }

    /** 查看全部键状态，可按调用方过滤 */
    @GetMapping("/keys")
    public List<KeyView> list(@RequestParam(required = false) String callerId) {
        return gateway.listStates().stream()
                .filter(v -> callerId == null || callerId.equals(v.key().callerId()))
                .toList();
    }

    /** 查看指定键状态 */
    @GetMapping("/keys/{callerId}/{key}")
    public ResponseEntity<KeyView> view(@PathVariable String callerId, @PathVariable String key) {
        return gateway.viewState(callerId, key)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /** 清理指定键状态（在途执行不受影响） */
    @DeleteMapping("/keys/{callerId}/{key}")
    public ResponseEntity<Void> clear(@PathVariable String callerId, @PathVariable String key) {
        return gateway.clear(callerId, key)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }
}
