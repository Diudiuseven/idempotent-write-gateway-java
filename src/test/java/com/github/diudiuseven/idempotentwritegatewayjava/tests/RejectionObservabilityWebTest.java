package com.github.diudiuseven.idempotentwritegatewayjava.tests;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 容量拒绝在运维查看能力中的端到端可观测性验证（独立上下文：在途上限为 1）。
 */
@SpringBootTest(properties = "idempotency.max-in-flight=1")
@AutoConfigureMockMvc
class RejectionObservabilityWebTest {

    @Autowired
    private MockMvc mockMvc;

    private ExecutorService pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }

    private static String body(String caller, String key, long delayMillis) {
        return """
                {"caller":"%s","key":"%s","payload":"p","delayMillis":%d,"failMode":"NONE"}
                """.formatted(caller, key, delayMillis);
    }

    @Test
    void rejectedRequestIsQueryableWithRejectedStatusAndDoesNotBlockLaterRetry() throws Exception {
        pool = Executors.newSingleThreadExecutor();
        // 占用唯一在途名额（下游耗时 2s）
        Future<?> busy = pool.submit(() -> {
            try {
                mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                                .content(body("rj", "busy", 2000)))
                        .andExpect(status().isOk());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        return mockMvc.perform(get("/api/idempotency/keys/rj/busy"))
                                .andReturn().getResponse().getStatus() == 200;
                    } catch (Exception e) {
                        return false;
                    }
                });

        // 容量超限：另一个键被 429 拒绝
        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("rj", "turned-away", 0)))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.error").value("CAPACITY_EXCEEDED"));

        // 被拒绝的请求可在运维查看能力中查询到，且状态可区分
        mockMvc.perform(get("/api/idempotency/keys/rj/turned-away"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.failureMessage").isNotEmpty())
                .andExpect(jsonPath("$.expiresAt").isNotEmpty());

        mockMvc.perform(get("/api/idempotency/keys"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.status=='REJECTED')]").isNotEmpty());

        // 在途执行结束、容量恢复后，被拒绝的同键请求可正常处理
        busy.get(10, TimeUnit.SECONDS);
        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("rj", "turned-away", 0)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
        mockMvc.perform(get("/api/idempotency/keys/rj/turned-away"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }
}
