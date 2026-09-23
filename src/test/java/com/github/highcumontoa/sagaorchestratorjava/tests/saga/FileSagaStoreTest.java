package com.github.highcumontoa.sagaorchestratorjava.tests.saga;

import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaStatus;
import com.github.highcumontoa.sagaorchestratorjava.saga.store.FileSagaStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 本地文件存储：创建去重、版本冲突、重启扫描。 */
class FileSagaStoreTest {

    private static final Logger log = LoggerFactory.getLogger(FileSagaStoreTest.class);

    @TempDir
    Path tempDir;

    private SagaInstance instance(String id) {
        SagaInstance inst = new SagaInstance();
        inst.setInstanceId(id);
        inst.setType("t");
        inst.setStatus(SagaStatus.PENDING);
        return inst;
    }

    @Test
    void createIfAbsentIsIdempotentAndVersionConflictsAreRejected() {
        FileSagaStore store = new FileSagaStore(tempDir);

        assertTrue(store.createIfAbsent(instance("i1")).isEmpty(), "判定依据：首次创建成功");
        SagaInstance existing = store.createIfAbsent(instance("i1")).orElseThrow();
        log.info("[assert] second create returned existing version={}", existing.getVersion());
        assertEquals(0, existing.getVersion(), "判定依据：重复创建返回已存在实例而非覆盖");

        SagaInstance stale = store.find("i1").orElseThrow();
        stale.setStatus(SagaStatus.RUNNING);
        store.update(stale, 0L); // 版本推进到 1
        assertEquals(1, store.find("i1").orElseThrow().getVersion());

        SagaInstance staleCopy = instance("i1"); // version=0 的过期对象
        SagaRejectedException ex = assertThrows(SagaRejectedException.class,
                () -> store.update(staleCopy, 0L));
        log.info("[assert] stale update rejected reason={}: {}", ex.getReason(), ex.getMessage());
        assertEquals(RejectReason.VERSION_CONFLICT, ex.getReason(),
                "判定依据：并发冲突必须以 VERSION_CONFLICT 明确拒绝");
        assertEquals(SagaStatus.RUNNING, store.find("i1").orElseThrow().getStatus(),
                "判定依据：被拒绝的过期写入不得覆盖已提交状态");
    }

    @Test
    void dataSurvivesStoreReinitialisation() {
        new FileSagaStore(tempDir).createIfAbsent(instance("i2"));
        FileSagaStore reopened = new FileSagaStore(tempDir);
        assertEquals(1, reopened.findAll().size(), "判定依据：重新初始化后必须能从磁盘扫描到实例");
        assertEquals("i2", reopened.find("i2").orElseThrow().getInstanceId());
    }
}
