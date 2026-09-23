package com.github.highcumontoa.sagaorchestratorjava.tests;

import com.github.highcumontoa.sagaorchestratorjava.saga.action.SagaDefinition;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaDefinitionRegistry;
import com.github.highcumontoa.sagaorchestratorjava.saga.engine.SagaOrchestrator;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.InstanceStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaContext;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaRepository;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingAction;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.RecordingCompensation;
import com.github.highcumontoa.sagaorchestratorjava.tests.support.SagaFixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 场景八：本地文件存储在“组件重启（全新对象）”后仍可读出状态并继续推进。 */
class FilePersistenceRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(FilePersistenceRecoveryTest.class);

    @TempDir
    Path tempDir;

    @Test
    void stateSurvivesRestartAndCanContinueCompensation() {
        // 定义在“两个进程”中各注册一次（模拟重启后应用重新声明定义）
        SagaDefinition def1 = SagaFixtures.definition("file-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .step("s2", new RecordingAction("s2"), new RecordingCompensation("c2"))
                .build();
        // 动作实例不共享：重启后全新对象，唯一可靠状态只能来自磁盘
        SagaDefinition def2 = SagaFixtures.definition("file-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .step("s2", new RecordingAction("s2"), new RecordingCompensation("c2"))
                .build();

        SagaOrchestrator first = new SagaOrchestrator(
                new FileSagaRepository(tempDir), new SagaDefinitionRegistry());
        // 完成正向
        SagaInstance completed = first.submit(def1, "TX-FILE-1", new SagaContext());
        assertEquals(InstanceStatus.COMPLETED, completed.getStatus());
        log.info("第一次运行后状态=COMPLETED，存储目录={}", tempDir);

        // “重启”：全新 repository / registry / orchestrator，仅共享磁盘目录
        FileSagaRepository reopenedRepo = new FileSagaRepository(tempDir);
        SagaDefinitionRegistry reopenedRegistry = new SagaDefinitionRegistry();
        reopenedRegistry.register(def2);
        SagaOrchestrator second = new SagaOrchestrator(reopenedRepo, reopenedRegistry);

        SagaInstance loaded = second.get("TX-FILE-1");
        assertEquals(InstanceStatus.COMPLETED, loaded.getStatus());
        assertEquals(2, loaded.getSteps().size());
        log.info("重启后读出状态={}，轨迹条数={}", loaded.getStatus(), loaded.getTrace().size());
        assertTrue(loaded.getTrace().size() >= 5, "执行轨迹必须随持久化保留");

        // 恢复器不应触碰终态
        assertEquals(0, second.recoverAll());
        // 重启后显式补偿仍然有效，且轨迹连续可解释
        SagaInstance compensated = second.compensate("TX-FILE-1");
        assertEquals(InstanceStatus.COMPENSATED, compensated.getStatus());
        assertEquals(3, compensated.getTrace().stream()
                .filter(e -> e.detail().contains("COMPENSATED")).count(),
                "两条步骤补偿成功 + 一条实例终态");
        log.info("重启后补偿完成 -> COMPENSATED；总轨迹条数={}", compensated.getTrace().size());

        // 再重启：终态依旧，重复补偿幂等
        SagaDefinitionRegistry thirdRegistry = new SagaDefinitionRegistry();
        SagaDefinition def3 = SagaFixtures.definition("file-flow")
                .step("s1", new RecordingAction("s1"), new RecordingCompensation("c1"))
                .step("s2", new RecordingAction("s2"), new RecordingCompensation("c2"))
                .build();
        thirdRegistry.register(def3);
        SagaOrchestrator third = new SagaOrchestrator(new FileSagaRepository(tempDir), thirdRegistry);
        third.recoverAll();
        SagaInstance again = third.compensate("TX-FILE-1");
        assertEquals(InstanceStatus.COMPENSATED, again.getStatus());
        long versionBefore = again.getVersion();
        SagaInstance yetAgain = third.compensate("TX-FILE-1");
        assertEquals(versionBefore, yetAgain.getVersion(), "终态重复补偿不得产生新版本");
        log.info("再次重启+重复补偿收敛于 COMPENSATED，版本={} 不增长", versionBefore);
    }
}
