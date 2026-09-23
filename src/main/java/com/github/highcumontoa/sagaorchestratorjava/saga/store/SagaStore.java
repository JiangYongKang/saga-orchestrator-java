package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;

import java.util.List;
import java.util.Optional;

/**
 * 实例与幂等守卫的本地存储抽象。所有读写必须落盘，保证重启可恢复。
 */
public interface SagaStore {

    /** 仅当实例不存在时创建；已存在则返回空（供提交幂等判定）。 */
    Optional<SagaInstance> createIfAbsent(SagaInstance instance);

    Optional<SagaInstance> find(String instanceId);

    List<SagaInstance> findAll();

    /**
     * 带乐观版本号的条件更新。
     * @throws com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException
     *         版本不匹配时以 VERSION_CONFLICT 拒绝
     */
    void update(SagaInstance instance, long expectedVersion);

    Optional<GuardRecord> findGuard(String key);

    void saveGuard(GuardRecord record);
}
