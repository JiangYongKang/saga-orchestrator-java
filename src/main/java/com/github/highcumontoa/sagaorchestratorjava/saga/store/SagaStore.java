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

    /**
     * 尝试认领某个实例的恢复权（原子判定 + 落盘，跨编排器、跨进程有效）。
     *
     * <p>判定规则：认领文件不存在、已释放，或其中租约已过期（持有者崩溃/失联）时，
     * 当前调用者获得认领并返回 true；认领被其它持有者占用且租约未过期时返回 false，
     * 调用方应按「该实例已被别人推进」安静收敛，继续处理后续实例。
     *
     * @param ownerId    调用方（恢复者）的稳定标识
     * @param leaseMillis 租约时长；持有期间应通过 {@link #renewRecoveryClaim} 续租
     */
    boolean tryClaimRecovery(String instanceId, String ownerId, long leaseMillis);

    /** 持有者续租：仅当认领仍归 {@code ownerId} 所有时延长租约；否则静默忽略。 */
    void renewRecoveryClaim(String instanceId, String ownerId, long leaseMillis);

    /** 持有者释放认领：仅当认领仍归 {@code ownerId} 所有时删除；否则静默忽略。 */
    void releaseRecovery(String instanceId, String ownerId);
}
