package com.github.highcumontoa.sagaorchestratorjava.saga.config;

import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * 应用启动（或重新初始化）后自动恢复未完成实例。
 * 恢复严格依据落盘状态：已确认完成的步骤不会重复执行，错过的补偿不会被跳过。
 */
public class SagaStartupRecovery implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SagaStartupRecovery.class);

    private final SagaOrchestrator orchestrator;
    private final SagaProperties properties;

    public SagaStartupRecovery(SagaOrchestrator orchestrator, SagaProperties properties) {
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isRecoverOnStartup()) {
            log.info("[startup] saga recovery disabled by configuration");
            return;
        }
        int n = orchestrator.recoverAll();
        log.info("[startup] saga recovery finished, resumed {} unfinished instance(s)", n);
    }
}
