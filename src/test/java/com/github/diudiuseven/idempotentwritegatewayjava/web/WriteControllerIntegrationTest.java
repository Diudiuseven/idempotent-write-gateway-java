package com.github.diudiuseven.idempotentwritegatewayjava.web;

import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteResult;
import com.github.diudiuseven.idempotentwritegatewayjava.downstream.WriteService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 写入接口集成测试：HTTP 头传递幂等键、并发在途合并、参数校验。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "idempotency.downstream-delay=300ms")
class WriteControllerIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private WriteService writeService;

    private HttpEntity<String> entity(String callerId, String key, String payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(WriteController.HEADER_CALLER_ID, callerId);
        headers.set(WriteController.HEADER_IDEMPOTENCY_KEY, key);
        return new HttpEntity<>(payload, headers);
    }

    @Test
    void write_withIdempotencyHeaders_returnsResult() {
        ResponseEntity<WriteResult> resp = rest.postForEntity(
                "/api/writes", entity("callerA", "http-key-1", "hello"), WriteResult.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().writeId()).isNotBlank();
        assertThat(resp.getBody().echo()).isEqualTo("hello");
        assertThat(resp.getBody().processedAt()).isNotNull();
    }

    @Test
    void missingIdempotencyKeyHeader_returns400() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(WriteController.HEADER_CALLER_ID, "callerA");
        ResponseEntity<Map> resp = rest.postForEntity(
                "/api/writes", new HttpEntity<>("x", headers), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void missingCallerIdHeader_returns400() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(WriteController.HEADER_IDEMPOTENCY_KEY, "k");
        ResponseEntity<Map> resp = rest.postForEntity(
                "/api/writes", new HttpEntity<>("x", headers), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void concurrentPosts_sameKey_mergeIntoOneDownstreamCall() throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<ResponseEntity<WriteResult>>> futures = new ArrayList<>();
        long before = writeService.invocationCount();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> rest.postForEntity(
                        "/api/writes", entity("callerA", "http-merge-key", "payload"), WriteResult.class)));
            }
            String writeId = null;
            for (Future<ResponseEntity<WriteResult>> f : futures) {
                ResponseEntity<WriteResult> resp = f.get();
                assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(resp.getBody()).isNotNull();
                if (writeId == null) {
                    writeId = resp.getBody().writeId();
                } else {
                    // 所有等待方复用同一次执行结果
                    assertThat(resp.getBody().writeId()).isEqualTo(writeId);
                }
            }
        } finally {
            pool.shutdownNow();
        }
        // 下游副作用只发生一次
        assertThat(writeService.invocationCount() - before).isEqualTo(1);
    }

    @Test
    void sameKey_differentCallers_doNotMerge() {
        ResponseEntity<WriteResult> r1 = rest.postForEntity(
                "/api/writes", entity("callerA", "http-scope-key", "p"), WriteResult.class);
        ResponseEntity<WriteResult> r2 = rest.postForEntity(
                "/api/writes", entity("callerB", "http-scope-key", "p"), WriteResult.class);
        assertThat(r1.getBody().writeId()).isNotEqualTo(r2.getBody().writeId());
    }
}
