package com.github.highcumontoa.sagaorchestratorjava.saga.config;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.SagaStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

/** 本地编排组件的 Spring 装配。 */
@Configuration
@EnableConfigurationProperties(SagaProperties.class)
public class SagaAutoConfiguration {

    @Bean
    public SagaStore sagaStore(SagaProperties properties) {
        return new FileSagaStore(Paths.get(properties.getStoreDir()));
    }

    @Bean
    public SagaOrchestrator sagaOrchestrator(SagaStore sagaStore) {
        return new SagaOrchestrator(sagaStore);
    }

    @Bean
    public SagaStartupRecovery sagaStartupRecovery(SagaOrchestrator orchestrator, SagaProperties properties) {
        return new SagaStartupRecovery(orchestrator, properties);
    }
}
