package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 步骤级确定重试策略。
 *
 * <p>是否还有下一次尝试仅由 {@code usedAttempts}（已用次数）与 {@code maxAttempts}
 * （最大次数，含首次）决定，不含任何随机因素，因此结论确定、可解释：
 * 只要 {@code usedAttempts < maxAttempts} 就允许再试一次。退避为固定时长。
 */
public final class RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(RetryPolicy.class);

    private RetryPolicy() {
    }

    public static boolean hasNextAttempt(int usedAttempts, int maxAttempts) {
        return usedAttempts < maxAttempts;
    }

    public static void backoff(long millis) throws InterruptedException {
        if (millis <= 0) {
            return;
        }
        log.debug("[retry] fixed backoff {} ms", millis);
        Thread.sleep(millis);
    }
}
