package com.github.highcumontoa.sagaorchestratorjava.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaRecoveryRunner;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.InMemorySagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.SagaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Spring 装配：
 * <ul>
 *   <li>未配置 saga.storage-dir：使用内存存储（适合单元测试）；</li>
 *   <li>配置 saga.storage-dir：使用本地文件存储，可演示重启/重新初始化后的恢复。</li>
 * </ul>
 */
@Configuration
@EnableConfigurationProperties(SagaProperties.class)
public class SagaAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SagaAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public SagaRepository sagaRepository(SagaProperties properties) {
        if (properties.getStorageDir() != null && !properties.getStorageDir().isBlank()) {
            Path dir = Paths.get(properties.getStorageDir());
            log.info("saga repository: file storage at {}", dir.toAbsolutePath());
            return new FileSagaRepository(dir);
        }
        log.info("saga repository: in-memory storage (set saga.storage-dir to persist across restarts)");
        return new InMemorySagaRepository();
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaDefinitionRegistry sagaDefinitionRegistry() {
        return new SagaDefinitionRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaOrchestrator sagaOrchestrator(SagaRepository repository, SagaDefinitionRegistry registry) {
        return new SagaOrchestrator(repository, registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public SagaRecoveryRunner sagaRecoveryRunner(SagaOrchestrator orchestrator, SagaProperties properties) {
        return new SagaRecoveryRunner(orchestrator, properties.isRecoverOnStartup());
    }
}
