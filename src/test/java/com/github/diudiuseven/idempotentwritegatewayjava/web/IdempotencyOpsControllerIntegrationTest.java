package com.github.diudiuseven.idempotentwritegatewayjava.web;

import com.github.diudiuseven.idempotentwritegatewayjava.core.EntryState;
import com.github.diudiuseven.idempotentwritegatewayjava.core.KeyView;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 运维接口集成测试：查看/清理幂等键状态。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IdempotencyOpsControllerIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    private HttpEntity<String> writeEntity(String callerId, String key) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(WriteController.HEADER_CALLER_ID, callerId);
        headers.set(WriteController.HEADER_IDEMPOTENCY_KEY, key);
        return new HttpEntity<>("payload", headers);
    }

    @Test
    void viewListAndClearKeyState() {
        // 先产生一条已完成状态
        ResponseEntity<WriteResult> written = rest.postForEntity(
                "/api/writes", writeEntity("ops-caller", "ops-key-1"), WriteResult.class);
        assertThat(written.getStatusCode()).isEqualTo(HttpStatus.OK);

        // 查看单个键
        ResponseEntity<KeyView> view = rest.getForEntity(
                "/api/idempotency/keys/ops-caller/ops-key-1", KeyView.class);
        assertThat(view.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(view.getBody()).isNotNull();
        assertThat(view.getBody().state()).isEqualTo(EntryState.COMPLETED);
        assertThat(view.getBody().key().callerId()).isEqualTo("ops-caller");
        assertThat(view.getBody().key().key()).isEqualTo("ops-key-1");
        assertThat(view.getBody().result()).isNotNull();

        // 查看不存在的键 → 404
        ResponseEntity<KeyView> missing = rest.getForEntity(
                "/api/idempotency/keys/ops-caller/ops-missing", KeyView.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // 列表包含该键
        ResponseEntity<List<KeyView>> list = rest.exchange(
                "/api/idempotency/keys", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeyView>>() {});
        assertThat(list.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(list.getBody()).isNotNull();
        assertThat(list.getBody()).anySatisfy(v -> {
            assertThat(v.key().callerId()).isEqualTo("ops-caller");
            assertThat(v.key().key()).isEqualTo("ops-key-1");
        });

        // 清理
        ResponseEntity<Void> deleted = rest.exchange(
                "/api/idempotency/keys/ops-caller/ops-key-1", HttpMethod.DELETE, null, Void.class);
        assertThat(deleted.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        // 清理后再查 → 404，重复清理 → 404
        assertThat(rest.getForEntity("/api/idempotency/keys/ops-caller/ops-key-1", KeyView.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.exchange("/api/idempotency/keys/ops-caller/ops-key-1", HttpMethod.DELETE, null, Void.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void list_canFilterByCallerId() {
        rest.postForEntity("/api/writes", writeEntity("ops-filter-a", "k1"), WriteResult.class);
        rest.postForEntity("/api/writes", writeEntity("ops-filter-b", "k2"), WriteResult.class);

        ResponseEntity<List<KeyView>> resp = rest.exchange(
                "/api/idempotency/keys?callerId=ops-filter-a", HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeyView>>() {});
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody()).allSatisfy(v ->
                assertThat(v.key().callerId()).isEqualTo("ops-filter-a"));
        assertThat(resp.getBody()).anySatisfy(v ->
                assertThat(v.key().key()).isEqualTo("k1"));
    }
}
