package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.saga.web.SagaController;
import com.github.highcumontoa.sagaorchestratorjava.saga.web.SagaExceptionHandler;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 场景十：HTTP 接口可查询状态/轨迹，并以不同状态码与 reason 区分拒绝原因。 */
class SagaWebApiTest {

    private static final Logger log = LoggerFactory.getLogger(SagaWebApiTest.class);

    private MockMvc mvc;
    private SagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();
        orchestrator = new SagaOrchestrator(repo, registry);
        mvc = MockMvcBuilders.standaloneSetup(new SagaController(orchestrator, registry))
                .setControllerAdvice(new SagaExceptionHandler())
                .build();

        SagaDefinition def = SagaFixtures.definition("web-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .step("s2", RecordingAction.failAlways("s2"), null)
                .build();
        SagaInstance instance = orchestrator.submit(def, "TX-WEB-1", new SagaContext());
        log.info("准备数据: TX-WEB-1 status={}（将用于查询）", instance.getStatus());
    }

    @Test
    void queryStatusAndTrace() throws Exception {
        mvc.perform(get("/sagas/TX-WEB-1").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.instanceId").value("TX-WEB-1"))
                .andExpect(jsonPath("$.status").value(InstanceStatus.COMPENSATED.name()))
                .andExpect(jsonPath("$.steps[0].status").value(StepStatusValue()))
                .andExpect(jsonPath("$.trace").isArray());
        mvc.perform(get("/sagas/TX-WEB-1/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].eventType").value("INSTANCE_SUBMITTED"));
        log.info("判定: 状态与轨迹接口返回可解释的 JSON（含 INSTANCE_SUBMITTED 等事件）");
    }

    private static String StepStatusValue() {
        return com.github.highcumontoa.sagaorchestratorjava.saga.model.StepStatus.COMPENSATED.name();
    }

    @Test
    void unknownInstanceReturns404WithReason() throws Exception {
        mvc.perform(get("/sagas/MISSING"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.reason").value("UNKNOWN_INSTANCE"));
        log.info("判定: 未知实例 -> 404 reason=UNKNOWN_INSTANCE");
    }

    @Test
    void illegalAndConflictAndIdempotentPaths() throws Exception {
        // 新建一个 RUNNING 实例：对其补偿属于非法跳转
        InMemorySagaRepository repo = new InMemorySagaRepository();
        SagaDefinitionRegistry registry = new SagaDefinitionRegistry();
        SagaOrchestrator orch2 = new SagaOrchestrator(repo, registry);
        SagaDefinition def = SagaFixtures.definition("web-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .build();
        registry.register(def);
        SagaInstance running = new SagaInstance();
        running.setInstanceId("TX-WEB-RUN");
        running.setDefinitionName("web-flow");
        running.setStatus(InstanceStatus.RUNNING);
        running.setCreatedAt(java.time.Instant.now());
        running.setUpdatedAt(running.getCreatedAt());
        running.getSteps().add(new com.github.highcumontoa.sagaorchestratorjava.saga.model.StepState(0, "s1"));
        repo.insertIfAbsent(running);

        MockMvc mvc2 = MockMvcBuilders.standaloneSetup(new SagaController(orch2, registry))
                .setControllerAdvice(new SagaExceptionHandler())
                .build();
        mvc2.perform(post("/sagas/TX-WEB-RUN/compensate"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("ILLEGAL_TRANSITION"));
        log.info("判定: RUNNING 下触发补偿 -> 409 reason=ILLEGAL_TRANSITION");

        // 已 COMPENSATED 的实例重复补偿：幂等 200，状态不变
        mvc.perform(post("/sagas/TX-WEB-1/compensate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(InstanceStatus.COMPENSATED.name()));
        log.info("判定: 已 COMPENSATED 重复补偿 -> 200 幂等返回");
    }
}
