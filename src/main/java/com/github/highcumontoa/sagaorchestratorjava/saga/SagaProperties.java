package com.github.highcumontoa.sagaorchestratorjava.saga;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 本地编排组件配置。 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    /** 本地文件存储目录（为空则使用内存存储，适合测试）。 */
    private String storageDir = "";

    /** 启动时是否自动恢复未完成实例。 */
    private boolean recoverOnStartup = true;

    public String getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(String storageDir) {
        this.storageDir = storageDir;
    }

    public boolean isRecoverOnStartup() {
        return recoverOnStartup;
    }

    public void setRecoverOnStartup(boolean recoverOnStartup) {
        this.recoverOnStartup = recoverOnStartup;
    }
}
