package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * 基于本地文件系统（JSON）的存储实现，用本地磁盘替代真实中间件。
 *
 * <p>一致性手段：
 * <ul>
 *   <li>每个实例 / 守卫键一把进程内锁，保证 create-if-absent 与条件更新的原子性；</li>
 *   <li>先写临时文件再原子 rename，保证落盘内容不会出现半截 JSON；</li>
 *   <li>实例带单调递增 version，更新时做乐观比较，冲突抛 {@link RejectReason#VERSION_CONFLICT}；</li>
 *   <li>findAll 直接扫描目录，重启后所有已落盘实例与守卫记录都可恢复。</li>
 * </ul>
 */
public class FileSagaStore implements SagaStore {

    private static final Logger log = LoggerFactory.getLogger(FileSagaStore.class);

    private final Path instanceDir;
    private final Path guardDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public FileSagaStore(Path baseDir) {
        this.instanceDir = baseDir.resolve("instances");
        this.guardDir = baseDir.resolve("guards");
        try {
            Files.createDirectories(instanceDir);
            Files.createDirectories(guardDir);
        } catch (IOException e) {
            throw new UncheckedIOException("init saga store dir failed: " + baseDir, e);
        }
        log.info("[store] initialised local file store at {}", baseDir.toAbsolutePath());
    }

    private ReentrantLock lockFor(String key) {
        return locks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    private Path instanceFile(String instanceId) {
        return instanceDir.resolve(safe(instanceId) + ".json");
    }

    private Path guardFile(String key) {
        return guardDir.resolve(safe(key) + ".json");
    }

    private static String safe(String raw) {
        return raw.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    @Override
    public Optional<SagaInstance> createIfAbsent(SagaInstance instance) {
        ReentrantLock lock = lockFor("inst:" + instance.getInstanceId());
        lock.lock();
        try {
            Path file = instanceFile(instance.getInstanceId());
            if (Files.exists(file)) {
                log.debug("[store] createIfAbsent hit existing instance {}", instance.getInstanceId());
                return Optional.of(readInstance(file));
            }
            instance.setVersion(0);
            writeJson(file, instance);
            log.debug("[store] created new instance {} version=0", instance.getInstanceId());
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<SagaInstance> find(String instanceId) {
        Path file = instanceFile(instanceId);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        return Optional.of(readInstance(file));
    }

    @Override
    public List<SagaInstance> findAll() {
        try (Stream<Path> stream = Files.list(instanceDir)) {
            return stream
                    .filter(p -> p.toString().endsWith(".json"))
                    .map(this::readInstance)
                    .sorted(Comparator.comparingLong(SagaInstance::getCreatedMillis))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("scan instances failed", e);
        }
    }

    @Override
    public void update(SagaInstance instance, long expectedVersion) {
        ReentrantLock lock = lockFor("inst:" + instance.getInstanceId());
        lock.lock();
        try {
            Path file = instanceFile(instance.getInstanceId());
            SagaInstance current = readInstance(file);
            if (current.getVersion() != expectedVersion) {
                log.warn("[store] version conflict on {}: expected={} actual={}",
                        instance.getInstanceId(), expectedVersion, current.getVersion());
                throw new SagaRejectedException(RejectReason.VERSION_CONFLICT,
                        "instance " + instance.getInstanceId()
                                + " was modified concurrently: expected version " + expectedVersion
                                + " but actual " + current.getVersion());
            }
            instance.setVersion(expectedVersion + 1);
            writeJson(file, instance);
            log.debug("[store] updated instance {} -> version={}",
                    instance.getInstanceId(), instance.getVersion());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<GuardRecord> findGuard(String key) {
        Path file = guardFile(key);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(file.toFile(), GuardRecord.class));
        } catch (IOException e) {
            throw new UncheckedIOException("read guard failed: " + key, e);
        }
    }

    @Override
    public void saveGuard(GuardRecord record) {
        ReentrantLock lock = lockFor("guard:" + record.getKey());
        lock.lock();
        try {
            writeJson(guardFile(record.getKey()), record);
        } finally {
            lock.unlock();
        }
    }

    private SagaInstance readInstance(Path file) {
        try {
            return mapper.readValue(file.toFile(), SagaInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException("read instance failed: " + file, e);
        }
    }

    private void writeJson(Path target, Object value) {
        try {
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp." + Thread.currentThread().getId());
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), value);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("write json failed: " + target, e);
        }
    }
}
