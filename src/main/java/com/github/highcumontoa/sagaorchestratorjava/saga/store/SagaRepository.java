package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;

import java.util.List;
import java.util.Optional;

/**
 * 实例存储抽象（本地实现，替代真实中间件）。
 * 所有写入均为 CAS：expectedVersion 必须与当前版本一致，否则抛出版本冲突。
 */
public interface SagaRepository {

    /**
     * 条件插入：仅当实例不存在时成功。
     * @return true 表示插入成功；false 表示同 ID 实例已存在（重复提交）
     */
    boolean insertIfAbsent(SagaInstance instance);

    /** 按 ID 读取；不存在返回 empty。 */
    Optional<SagaInstance> find(String instanceId);

    /**
     CAS 更新：instance.version 必须等于当前版本，更新后版本 +1。
     */
    void compareAndUpdate(SagaInstance instance, long expectedVersion);

    /** 列出全部实例（恢复与测试校验用）。 */
    List<SagaInstance> findAll();
}
