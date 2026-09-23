package com.github.highcumontoa.sagaorchestratorjava.saga.web;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaSnapshot;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SubmitResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** 提交、补偿、状态与轨迹查询的 HTTP 入口。 */
@RestController
@RequestMapping("/saga")
public class SagaController {

    /** 提交请求体：{ "businessId": "...", "input": {...} }，instanceId 放路径，type 放路径。 */
    public record SubmitRequest(String businessId, Map<String, Object> input) {
    }

    private final SagaOrchestrator orchestrator;

    public SagaController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping("/{type}/{instanceId}")
    public SubmitResult submit(@PathVariable String type,
                               @PathVariable String instanceId,
                               @RequestBody(required = false) SubmitRequest request) {
        SubmitRequest req = request == null ? new SubmitRequest(null, Map.of()) : request;
        return orchestrator.submit(type, instanceId, req.businessId(), req.input());
    }

    @PostMapping("/instances/{instanceId}/compensation")
    public SagaSnapshot compensate(@PathVariable String instanceId) {
        return orchestrator.triggerCompensation(instanceId);
    }

    @GetMapping("/{instanceId}")
    public SagaSnapshot get(@PathVariable String instanceId) {
        return orchestrator.snapshot(instanceId);
    }

    @GetMapping
    public List<SagaSnapshot> list() {
        return orchestrator.list();
    }
}
