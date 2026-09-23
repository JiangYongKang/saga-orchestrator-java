package com.github.highcumontoa.sagaorchestratorjava.saga.web;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.IllegalSagaTransitionException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.TraceEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 对外提交/查询/操作实例的最小 HTTP 接口。 */
@RestController
@RequestMapping("/sagas")
public class SagaController {

    private final SagaOrchestrator orchestrator;
    private final SagaDefinitionRegistry registry;

    public SagaController(SagaOrchestrator orchestrator, SagaDefinitionRegistry registry) {
        this.orchestrator = orchestrator;
        this.registry = registry;
    }

    /** 提交并正向执行；同 instanceId 重复提交幂等收敛。 */
    @PostMapping
    public SagaInstance submit(@RequestBody SubmitRequest request) {
        var definition = registry.get(request.definitionName())
                .orElseThrow(() -> new IllegalSagaTransitionException(
                        "saga definition not registered: " + request.definitionName()));
        return orchestrator.submit(definition, request.instanceId(), new SagaContext());
    }

    /** 查询实例状态、步骤状态与失败原因。 */
    @GetMapping("/{instanceId}")
    public SagaInstance get(@PathVariable String instanceId) {
        return orchestrator.get(instanceId);
    }

    /** 查询执行轨迹（提交、状态迁移、每次尝试的结论与判定依据）。 */
    @GetMapping("/{instanceId}/trace")
    public List<TraceEvent> trace(@PathVariable String instanceId) {
        return orchestrator.getTrace(instanceId);
    }

    /** 幂等触发补偿。 */
    @PostMapping("/{instanceId}/compensate")
    public SagaInstance compensate(@PathVariable String instanceId) {
        return orchestrator.compensate(instanceId);
    }
}
