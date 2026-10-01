package com.github.diudiuseven.idempotentwritegatewayjava.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.diudiuseven.idempotentwritegatewayjava.gateway.DownstreamWriteService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class WriteGatewayWebTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DownstreamWriteService downstream;

    private static String body(String caller, String key, String failMode) {
        return """
                {"caller":"%s","key":"%s","payload":"p","delayMillis":0,"failMode":"%s"}
                """.formatted(caller, key, failMode);
    }

    @Test
    void duplicateSubmissionWithinTtlTriggersDownstreamOnce() throws Exception {
        int before = downstream.invocationCount();

        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-a", "dup-1", "NONE")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.output").value("PROCESSED:p"));

        // 客户端重试：命中缓存，不再触发下游
        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-a", "dup-1", "NONE")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.output").value("PROCESSED:p"));

        assertThat(downstream.invocationCount()).isEqualTo(before + 1);
    }

    @Test
    void concurrentDuplicatePostsTriggerDownstreamOnce() throws Exception {
        int before = downstream.invocationCount();
        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<MvcResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> mockMvc.perform(post("/api/writes")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body("web-b", "conc-1", "NONE")))
                    .andExpect(status().isOk())
                    .andReturn()));
        }
        for (Future<MvcResult> f : futures) {
            assertThat(f.get().getResponse().getContentAsString()).contains("PROCESSED:p");
        }
        pool.shutdownNow();
        assertThat(downstream.invocationCount()).isEqualTo(before + 1);
    }

    @Test
    void fatalFailureReturns500AndIsNotReplayed() throws Exception {
        int before = downstream.invocationCount();

        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-c", "fatal-1", "FATAL")))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("FatalDownstreamException"));

        // 有效期内重放失败，不再次触发下游
        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-c", "fatal-1", "FATAL")))
                .andExpect(status().isInternalServerError());

        assertThat(downstream.invocationCount()).isEqualTo(before + 1);
    }

    @Test
    void retryableFailureAllowsNextAttempt() throws Exception {
        int before = downstream.invocationCount();

        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-d", "retry-1", "RETRYABLE")))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("TransientDownstreamException"));

        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-d", "retry-1", "NONE")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.output").value("PROCESSED:p"));

        assertThat(downstream.invocationCount()).isEqualTo(before + 2);
    }

    @Test
    void adminEndpointsExposeAndEvictKeyState() throws Exception {
        mockMvc.perform(post("/api/writes").contentType(MediaType.APPLICATION_JSON)
                        .content(body("web-e", "admin-1", "NONE")))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/idempotency/keys/web-e/admin-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.caller").value("web-e"));

        mockMvc.perform(get("/api/idempotency/keys"))
                .andExpect(status().isOk());

        mockMvc.perform(delete("/api/idempotency/keys/web-e/admin-1"))
                .andExpect(status().isNoContent());
        mockMvc.perform(get("/api/idempotency/keys/web-e/admin-1"))
                .andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/idempotency/keys/web-e/admin-1"))
                .andExpect(status().isNotFound());
    }
}
