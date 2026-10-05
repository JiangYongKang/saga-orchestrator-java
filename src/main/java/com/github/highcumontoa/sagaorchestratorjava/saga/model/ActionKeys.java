package com.github.highcumontoa.sagaorchestratorjava.saga.model;

/**
 * 稳定动作去重标识（action key）的派生规则。
 *
 * <p>标识完全由已持久化的字段（type / instanceId / stepName / 阶段）确定性派生，
 * 因此：
 * <ul>
 *   <li>同一逻辑动作无论重试多少次、重启多少次、被重复触发多少次，标识都不变；</li>
 *   <li>不同实例 / 不同步骤 / 正向与补偿之间的标识互不撞车；</li>
 *   <li>对功能上线前已落盘的老实例同样适用——无需迁移，恢复时现场派生即可。</li>
 * </ul>
 *
 * <p>业务方在动作实现里通过 {@link ActionContext#actionKey()} 拿到该标识，
 * 用它做业务侧判重（例如以它为键写去重表），即可在「副作用已落地但确认丢失」
 * 时自己挡掉重复生效。
 */
public final class ActionKeys {

    /** 正向动作阶段名。 */
    public static final String PHASE_FORWARD = "FORWARD";
    /** 补偿动作阶段名。 */
    public static final String PHASE_COMPENSATE = "COMPENSATE";

    private ActionKeys() {
    }

    /** 正向动作的稳定去重标识。 */
    public static String forward(String type, String instanceId, String stepName) {
        return of(type, instanceId, stepName, PHASE_FORWARD);
    }

    /** 补偿动作的稳定去重标识。 */
    public static String compensation(String type, String instanceId, String stepName) {
        return of(type, instanceId, stepName, PHASE_COMPENSATE);
    }

    /** 通用派生：{@code saga:<type>:<instanceId>:<stepName>:<phase>}。 */
    public static String of(String type, String instanceId, String stepName, String phase) {
        return "saga:" + type + ":" + instanceId + ":" + stepName + ":" + phase;
    }
}
