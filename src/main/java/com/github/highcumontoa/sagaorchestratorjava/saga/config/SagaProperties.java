package com.github.highcumontoa.sagaorchestratorjava.saga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 编排组件本地配置。 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    /** 本地持久化目录。 */
    private String storeDir = "target/saga-store";

    /** 启动时是否自动恢复未完成实例。 */
    private boolean recoverOnStartup = true;

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
}
