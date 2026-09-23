package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端集成测试：验证 Spring 自动装配、启动恢复、HTTP 提交/查询/补偿与拒绝码。
 * 不依赖任何外部服务，持久化使用临时目录。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SagaIntegrationTest.class);

    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        try {
            tempDir = Files.createTempDirectory("saga-it-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        registry.add("saga.store-dir", () -> tempDir.toString());
        registry.add("saga.recover-on-startup", () -> "true");
    }

    @LocalServerPort
    int port;

    @Autowired
    SagaOrchestrator orchestrator;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    void registerDefinitions() {
        orchestrator.register(new SagaDefinition("http-flow", List.of(
                new StepDefinition("s1", ctx -> "R1", null, 1, 0L, 0L),
                new StepDefinition("s2", ctx -> "R2", null, 1, 0L, 0L))));
        orchestrator.register(new SagaDefinition("http-fail", List.of(
                new StepDefinition("ok", ctx -> "OK",
                        ctx -> log.info("[integration] compensating ok"), 1, 0L, 0L),
                new StepDefinition("bad", ctx -> {
                    throw new IllegalStateException("kaboom");
                }, null, 1, 0L, 0L))));
    }

    private String url(String path) {
        return "http://localhost:" + port + "/saga" + path;
    }

    @Test
    void submitQueryAndDuplicateViaHttp() {
        Map<String, Object> body = Map.of("businessId", "BIZ-HTTP-1", "input", Map.of("k", "v"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map> first = rest.postForEntity(url("/http-flow/inst-http-1"), body, Map.class);
        log.info("[http] first submit status={} body={}", first.getStatusCode(), first.getBody());
        assertEquals(HttpStatus.OK, first.getStatusCode());
        assertEquals("COMPLETED", ((Map<?, ?>) first.getBody().get("snapshot")).get("status"));
        assertEquals(Boolean.FALSE, first.getBody().get("deduplicated"));

        @SuppressWarnings("unchecked")
        ResponseEntity<Map> second = rest.postForEntity(url("/http-flow/inst-http-1"), body, Map.class);
        log.info("[http] duplicate submit body={}", second.getBody());
        assertEquals(Boolean.TRUE, second.getBody().get("deduplicated"),
                "判定依据：HTTP 重复提交必须显式返回 deduplicated=true");

        @SuppressWarnings("unchecked")
        ResponseEntity<Map> got = rest.getForEntity(url("/inst-http-1"), Map.class);
        List<?> steps = (List<?>) got.getBody().get("steps");
        assertEquals(2, steps.size());
        log.info("[http] snapshot status={} version={}", got.getBody().get("status"), got.getBody().get("version"));
        assertEquals("COMPLETED", got.getBody().get("status"));
    }

    @Test
    void failureCompensatesViaHttpAndRejectionCodesAreDistinguishable() {
        // 准备一个已完成实例（不依赖其他测试方法的执行顺序）
        rest.postForEntity(url("/http-flow/inst-http-illegal"),
                Map.of("businessId", "BIZ-IL"), Map.class);

        @SuppressWarnings("unchecked")
        ResponseEntity<Map> fail = rest.postForEntity(url("/http-fail/inst-http-2"),
                Map.of("businessId", "BIZ-HTTP-2"), Map.class);
        log.info("[http] failing flow status={} body={}", fail.getStatusCode(), fail.getBody());
        assertEquals("COMPENSATED", ((Map<?, ?>) fail.getBody().get("snapshot")).get("status"),
                "判定依据：失败后唯一成功步骤补偿完成 -> COMPENSATED");

        // 未知实例 -> 404 UNKNOWN_INSTANCE
        ResponseEntity<String> unknown = rest.getForEntity(url("/does-not-exist"), String.class);
        log.info("[http] unknown -> {} {}", unknown.getStatusCode(), unknown.getBody());
        assertEquals(HttpStatus.NOT_FOUND, unknown.getStatusCode());
        assertTrue(unknown.getBody().contains("UNKNOWN_INSTANCE"));

        // 对已完成实例发起补偿 -> 422 ILLEGAL_STATE
        ResponseEntity<String> illegal = rest.postForEntity(url("/instances/inst-http-illegal/compensation"),
                HttpEntity.EMPTY, String.class);
        log.info("[http] illegal compensate -> {} {}", illegal.getStatusCode(), illegal.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, illegal.getStatusCode());
        assertTrue(illegal.getBody().contains("ILLEGAL_STATE"),
                "判定依据：非法状态跳转必须以 ILLEGAL_STATE 可区分地拒绝");

        // 验证失败实例的步骤状态可解释：bad=FAILED, ok=COMPENSATED
        @SuppressWarnings("unchecked")
        ResponseEntity<Map> snap = rest.getForEntity(url("/inst-http-2"), Map.class);
        List<Map<?, ?>> steps = (List<Map<?, ?>>) snap.getBody().get("steps");
        assertEquals(StepStatus.COMPENSATED.name(), steps.get(0).get("status"));
        assertEquals(StepStatus.FAILED.name(), steps.get(1).get("status"));
        List<?> traces = (List<?>) snap.getBody().get("traces");
        assertFalse(traces.isEmpty(), "判定依据：必须对外提供执行轨迹");
    }

    @Test
    void unknownSagaTypeRejected() {
        ResponseEntity<String> resp = rest.postForEntity(url("/no-such-type/inst-http-3"),
                Map.of(), String.class);
        log.info("[http] unknown type -> {} {}", resp.getStatusCode(), resp.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        assertTrue(resp.getBody().contains("ILLEGAL_STATE"));
    }
}
