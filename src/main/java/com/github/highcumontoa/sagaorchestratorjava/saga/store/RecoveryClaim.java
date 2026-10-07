package com.github.highcumontoa.sagaorchestratorjava.saga.store;

/**
 * 恢复认领记录：某个恢复者（服务实例 / 启动自动恢复 / 运维手动触发）
 * 正在推进某个实例的凭证。落盘在共享存储中，跨编排器、跨进程可见。
 *
 * <p>语义：
 * <ul>
 *   <li>认领带租约（{@code leaseUntilMillis}）：持有者存活期间会在每次状态落盘时续租；</li>
 *   <li>其它恢复者看到未过期认领时，按「该实例已被别人推进」安静收敛，继续处理后续实例；</li>
 *   <li>持有者崩溃/失联导致租约过期后，任何恢复者都可接管，实例不会被漏掉；</li>
 *   <li>推进结束（无论成败）即释放认领，后续手动补跑不受阻。</li>
 * </ul>
 */
public class RecoveryClaim {

    private String instanceId;
    private String ownerId;
    private long acquiredMillis;
    private long leaseUntilMillis;

    public RecoveryClaim() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public long getAcquiredMillis() {
        return acquiredMillis;
    }

    public void setAcquiredMillis(long acquiredMillis) {
        this.acquiredMillis = acquiredMillis;
    }

    public long getLeaseUntilMillis() {
        return leaseUntilMillis;
    }

    public void setLeaseUntilMillis(long leaseUntilMillis) {
        this.leaseUntilMillis = leaseUntilMillis;
    }
}
