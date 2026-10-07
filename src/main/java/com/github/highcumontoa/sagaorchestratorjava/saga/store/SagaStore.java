package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 实例与幂等守卫的本地存储抽象。所有读写必须落盘，保证重启可恢复。
 *
 * <p>多个恢复者可以是同一 JVM 内的多个编排器，也可以是共享同一持久化目录的多个 JVM。
 * 因此本接口提供「守卫键级别的跨进程临界区」{@link #withGuardLock}：守卫的
 * 判状态 / 落盘变迁必须在该临界区内完成，保证两个恢复者不会同时穿透同一个守卫动作。
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

    /**
     * 在「该守卫键专属」的临界区内执行一段读取-判定-落盘逻辑。
     *
     * <p>默认实现不做跨进程互斥（适用于内存 / 单 JVM 实现）；文件存储实现用 OS 文件锁，
     * 保证共享同一份持久化的多个 JVM 之间，同一守卫键的临界区也严格串行；
     * 进程崩溃时锁由 OS 自动释放，不会留下死锁。临界区内不得再嵌套获取同一把守卫锁。
     */
    default <T> T withGuardLock(String guardKey, Supplier<T> section) {
        return section.get();
    }
}
