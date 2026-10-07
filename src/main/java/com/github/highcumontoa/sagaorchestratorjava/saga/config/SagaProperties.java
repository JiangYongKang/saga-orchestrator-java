package com.github.highcumontoa.sagaorchestratorjava.saga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 编排组件本地配置。 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    /** 本地持久化目录。 */
    private String storeDir = "target/saga-store";

    /** 启动时是否自动恢复未完成实例。 */
    private boolean recoverOnStartup = true;

    /**
     * 恢复认领的租约时长（毫秒）：多恢复者共享同一存储时，持有认领的恢复者在每次
     * 状态落盘时续租；持有者崩溃后租约到期，其它恢复者即可接管，实例不会被漏掉。
     */
    private long recoveryClaimLeaseMillis = 60_000L;

    public String getStoreDir() {
        return storeDir;
    }

    public void setStoreDir(String storeDir) {
        this.storeDir = storeDir;
    }

    public boolean isRecoverOnStartup() {
        return recoverOnStartup;
    }

    public void setRecoverOnStartup(boolean recoverOnStartup) {
        this.recoverOnStartup = recoverOnStartup;
    }

    public long getRecoveryClaimLeaseMillis() {
        return recoveryClaimLeaseMillis;
    }

    public void setRecoveryClaimLeaseMillis(long recoveryClaimLeaseMillis) {
        this.recoveryClaimLeaseMillis = recoveryClaimLeaseMillis;
    }
}
