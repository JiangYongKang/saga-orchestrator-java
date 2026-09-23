package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaConcurrentConflictException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * 基于本地文件的持久化存储，用于演示“重启/重新初始化后恢复”。
 *
 * <p>每个实例一个 JSON 文件 {@code <dir>/instances/<encodedId>.json}：
 * <ul>
 *   <li>写入：先写同目录临时文件，再原子 move 替换，保证读到的永远是完整快照；</li>
 *   <li>CAS：按实例取锁后重新读盘复查版本号，版本不一致抛 CONCURRENT_CONFLICT；</li>
 *   <li>删除：本组件不提供删除接口，历史实例（含轨迹）始终保留可查。</li>
 * </ul>
 */
public class FileSagaRepository implements SagaRepository {

    private static final Logger log = LoggerFactory.getLogger(FileSagaRepository.class);
    private static final int LOCK_STRIPES = 16;

    private final Path instancesDir;
    private final ReentrantLock[] locks;

    public FileSagaRepository(Path storageDir) {
        try {
            this.instancesDir = storageDir.resolve("instances");
            Files.createDirectories(instancesDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create storage dir: " + storageDir, e);
        }
        this.locks = new ReentrantLock[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) {
            this.locks[i] = new ReentrantLock();
        }
        log.info("FileSagaRepository initialized at {}", instancesDir.toAbsolutePath());
    }

    private Path fileFor(String instanceId) {
        String encoded = URLEncoder.encode(instanceId, StandardCharsets.UTF_8);
        return instancesDir.resolve(encoded + ".json");
    }

    private ReentrantLock lockFor(String instanceId) {
        return locks[Math.floorMod(instanceId.hashCode(), LOCK_STRIPES)];
    }

    @Override
    public boolean insertIfAbsent(SagaInstance instance) {
        ReentrantLock lock = lockFor(instance.getInstanceId());
        lock.lock();
        try {
            Path target = fileFor(instance.getInstanceId());
            if (Files.exists(target)) {
                return false;
            }
            Path tmp = null;
            try {
                tmp = Files.createTempFile(instancesDir, "saga-", ".tmp");
                writeFile(tmp, instance);
                Files.move(tmp, target);
            } catch (IOException e) {
                deleteQuietly(tmp);
                // 目标已被并发创建：按重复插入处理
                if (Files.exists(target)) {
                    return false;
                }
                throw new UncheckedIOException("insert failed for " + instance.getInstanceId(), e);
            }
            log.debug("persisted new instance {} version={}", instance.getInstanceId(), instance.getVersion());
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<SagaInstance> find(String instanceId) {
        Path file = fileFor(instanceId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            return Optional.of(Json.MAPPER.readValue(bytes, SagaInstance.class));
        } catch (IOException e) {
            throw new UncheckedIOException("read failed for " + instanceId, e);
        }
    }

    @Override
    public void compareAndUpdate(SagaInstance instance, long expectedVersion) {
        ReentrantLock lock = lockFor(instance.getInstanceId());
        lock.lock();
        try {
            Path target = fileFor(instance.getInstanceId());
            if (!Files.exists(target)) {
                throw new SagaConcurrentConflictException(instance.getInstanceId(), expectedVersion, -1L);
            }
            SagaInstance current = readFile(target);
            if (current.getVersion() != expectedVersion) {
                throw new SagaConcurrentConflictException(instance.getInstanceId(),
                        expectedVersion, current.getVersion());
            }
            instance.setVersion(expectedVersion + 1);
            Path tmp = null;
            try {
                tmp = Files.createTempFile(instancesDir, "saga-", ".tmp");
                writeFile(tmp, instance);
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                deleteQuietly(tmp);
                throw new UncheckedIOException("update failed for " + instance.getInstanceId(), e);
            }
            log.debug("persisted update of {} version={}", instance.getInstanceId(), instance.getVersion());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<SagaInstance> findAll() {
        List<SagaInstance> all = new ArrayList<>();
        try (Stream<Path> files = Files.list(instancesDir)) {
            files.filter(p -> p.toString().endsWith(".json")).forEach(p -> all.add(readFile(p)));
        } catch (IOException e) {
            throw new UncheckedIOException("list failed", e);
        }
        return all;
    }

    private static void writeFile(Path file, SagaInstance instance) throws IOException {
        Json.MAPPER.writer(new DefaultPrettyPrinter()).writeValue(file.toFile(), instance);
    }

    private static SagaInstance readFile(Path file) {
        try {
            return Json.MAPPER.readValue(file.toFile(), SagaInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException("read failed for " + file, e);
        }
    }

    private static void deleteQuietly(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // 临时文件清理失败不影响主流程
        }
    }
}
