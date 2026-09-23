package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 * 启动恢复器：应用完全就绪（所有编排定义 Bean 均已注册）后调用
 * orchestrator.recoverAll()，继续推进 RUNNING / COMPENSATING / COMPENSATION_FAILED
 * 的实例。可通过 saga.recover-on-startup=false 关闭（测试中便于手动控制恢复时机）。
 */
public class SagaRecoveryRunner {

    private static final Logger log = LoggerFactory.getLogger(SagaRecoveryRunner.class);

    private final SagaOrchestrator orchestrator;
    private final boolean recoverOnStartup;

    public SagaRecoveryRunner(SagaOrchestrator orchestrator, boolean recoverOnStartup) {
        this.orchestrator = orchestrator;
        this.recoverOnStartup = recoverOnStartup;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recoverOnStartup() {
        if (!recoverOnStartup) {
            log.info("saga auto recovery disabled (saga.recover-on-startup=false)");
            return;
        }
        int count = orchestrator.recoverAll();
        log.info("saga startup recovery finished, resumed instances={}", count);
    }
}
