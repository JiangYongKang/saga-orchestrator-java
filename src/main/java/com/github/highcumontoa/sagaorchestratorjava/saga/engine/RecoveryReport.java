package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import java.util.List;

/**
 * 一次 {@code recoverAll} 的可解释结论。
 *
 * <p>多个恢复者（启动自动恢复、运维手动恢复、多个实例共享同一份存储）并发跑时，
 * 每个恢复调用都正常跑完并返回本报告，不会抛错、不会因为某一个实例的冲突而中断整批：
 * <ul>
 *   <li>{@link #recovered()}：本调用真正推进（写盘恢复结论并跑完状态机）的实例；</li>
 *   <li>{@link #deferred()}：进入恢复时仍未完成，但处理期间已被并发的其它恢复者推进
 *       （CAS 写冲突败北 / 锁内重读发现已是终态或被接管），本调用按「该实例已被别人推进」
 *       安静跳过，继续处理后面的实例；</li>
 *   <li>{@link #alreadyTerminal()}：扫描时就已是终态的实例；</li>
 *   <li>{@link #awaitingDefinition()}：本进程尚未注册对应类型定义、暂缓恢复的实例；</li>
 *   <li>{@link #failed()}：因其它异常未能推进的实例（异常被隔离在单实例内，不中断整批）。</li>
 * </ul>
 *
 * @param recovered          本调用真正推进的实例 id
 * @param deferred           冲突认输、由其它恢复者推进的实例 id
 * @param alreadyTerminal    扫描时已是终态的实例 id
 * @param awaitingDefinition 定义缺失、暂缓恢复的实例 id
 * @param failed             发生其它异常的实例 id
 */
public record RecoveryReport(List<String> recovered,
                             List<String> deferred,
                             List<String> alreadyTerminal,
                             List<String> awaitingDefinition,
                             List<String> failed,
                             int scanned) {
}
