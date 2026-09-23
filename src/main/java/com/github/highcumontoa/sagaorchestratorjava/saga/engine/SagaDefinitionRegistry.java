package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 编排定义注册表：定义名 -> 定义。恢复时据此重建执行器所需的步骤信息。 */
public class SagaDefinitionRegistry {

    private final ConcurrentHashMap<String, SagaDefinition> definitions = new ConcurrentHashMap<>();

    public void register(SagaDefinition definition) {
        SagaDefinition previous = definitions.putIfAbsent(definition.name(), definition);
        if (previous != null && !previous.equals(definition)) {
            throw new IllegalStateException("saga definition already registered with different content: "
                    + definition.name());
        }
    }

    public Optional<SagaDefinition> get(String name) {
        return Optional.ofNullable(definitions.get(name));
    }
}
