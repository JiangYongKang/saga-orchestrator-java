package com.github.highcumontoa.sagaorchestratorjava.saga.engine;

import com.github.highcumontoa.sagaorchestratorjava.saga.model.TimeoutPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 以确定的超时策略执行一次动作调用。
 *
 * <p>动作在独立虚拟线程上执行；在 {@link TimeoutPolicy#timeout()} 内未返回
 * 即判定为 TIMEOUT（取消执行线程）。TIMEOUT 属于“不确定结论”：动作可能已生效，
 * 是否重复生效由业务动作凭 executionId 幂等保证。
 */
public class AttemptRunner {

    private static final Logger log = LoggerFactory.getLogger(AttemptRunner.class);

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public AttemptResult run(ThrowingAction action, TimeoutPolicy timeoutPolicy, String executionId) {
        Future<?> future = executor.submit(() -> {
            action.run();
            return null;
        });
        try {
            future.get(timeoutPolicy.timeout().toMillis(), TimeUnit.MILLISECONDS);
            return AttemptResult.success();
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn("attempt {} timed out after {}", executionId, timeoutPolicy.timeout());
            return AttemptResult.timeout("timeout after " + timeoutPolicy.timeout());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.warn("attempt {} failed: {}", executionId, cause.toString());
            return AttemptResult.failed(cause.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return AttemptResult.failed("interrupted while awaiting attempt " + executionId);
        } catch (CancellationException e) {
            return AttemptResult.timeout("attempt cancelled by timeout " + executionId);
        }
    }

    public void shutdown() {
        executor.shutdownNow();
    }

    @FunctionalInterface
    public interface ThrowingAction {
        void run() throws Exception;
    }
}
