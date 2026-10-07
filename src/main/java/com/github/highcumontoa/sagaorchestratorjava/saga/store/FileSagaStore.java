package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.RejectReason;
import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaRejectedException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * 基于本地文件系统（JSON）的存储实现，用本地磁盘替代真实中间件。
 *
 * <p>一致性手段：
 * <ul>
 *   <li>每个实例 / 守卫键一把进程内锁，保证 create-if-absent 与条件更新在本 JVM 内的原子性；</li>
 *   <li>跨 JVM 的互斥用 OS 文件锁（{@code locks/} 目录，进程崩溃由 OS 自动释放）：
 *       {@code createIfAbsent} 在实例锁内「临时文件写全 + 复查存在 + 原子 rename 发布」，
 *       {@code update} 在实例锁内做「读版本 → 比较 → 写」，守卫的 begin/complete/fail 同理，
 *       多个 JVM 共享同一目录时也只有唯一创建者、唯一 CAS 获胜者；</li>
 *   <li>先写临时文件再原子 rename，保证落盘内容不会出现半截 JSON；临时文件名带 UUID，
 *       不同进程 / 线程并发写同一目标也不会撞临时文件；</li>
 *   <li>实例带单调递增 version，更新时做乐观比较，冲突抛 {@link RejectReason#VERSION_CONFLICT}；
 *       多个恢复者并发恢复同一实例时，败者由编排器按「已被别人推进」安静收敛（不重试、不报错、
 *       不中断整批恢复）；</li>
 *   <li>findAll 直接扫描目录，重启后所有已落盘实例与守卫记录都可恢复。</li>
 * </ul>
 */
public class FileSagaStore implements SagaStore {

    private static final Logger log = LoggerFactory.getLogger(FileSagaStore.class);

    private final Path instanceDir;
    private final Path guardDir;
    private final Path lockDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public FileSagaStore(Path baseDir) {
        this.instanceDir = baseDir.resolve("instances");
        this.guardDir = baseDir.resolve("guards");
        this.lockDir = baseDir.resolve("locks");
        try {
            Files.createDirectories(instanceDir);
            Files.createDirectories(guardDir);
            Files.createDirectories(lockDir);
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

    private Path instanceLockFile(String instanceId) {
        return lockDir.resolve(safe("inst:" + instanceId) + ".lock");
    }

    private Path guardLockFile(String key) {
        return lockDir.resolve(safe("guard:" + key) + ".lock");
    }

    /** 「JVM 内锁 + OS 文件锁」双层临界区句柄；try-with-resources 关闭即按反序释放。 */
    private final class CrossProcessLock implements AutoCloseable {
        private final ReentrantLock jvmLock;
        private final FileChannel channel;
        private final FileLock osLock;
        private final Path lockFile;

        private CrossProcessLock(String jvmLockName, Path lockFile) {
            this.jvmLock = lockFor(jvmLockName);
            this.lockFile = lockFile;
            this.jvmLock.lock();
            FileChannel opened = null;
            try {
                opened = FileChannel.open(lockFile,
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                this.channel = opened;
                this.osLock = acquireOsLock(opened, lockFile);
            } catch (IOException e) {
                if (opened != null) {
                    try {
                        opened.close();
                    } catch (IOException closeFailure) {
                        e.addSuppressed(closeFailure);
                    }
                }
                jvmLock.unlock();
                throw new UncheckedIOException("cross-process lock failed: " + lockFile, e);
            }
        }

        @Override
        public void close() {
            try {
                osLock.release();
            } catch (IOException e) {
                log.debug("[store] release OS lock failed for {}: {}", lockFile, e.toString());
            }
            try {
                channel.close();
            } catch (IOException e) {
                log.debug("[store] close lock channel failed for {}: {}", lockFile, e.toString());
            } finally {
                // 锁文件本身不承载任何状态（互斥语义由 OS 锁 + 本 JVM 锁提供），
                // 临界区结束且通道关闭后尝试删除，保持持久化目录干净：
                //  - 无并发者时成功删除，@TempDir / 运维清理都不会留下空锁目录；
                //  - 有并发者正持有该锁（其它通道 open 着同一个文件）时删除失败也完全无害，
                //    最后一个释放者会把它删掉；进程崩溃残留的空锁文件下次照常打开、重新加锁。
                try {
                    Files.deleteIfExists(lockFile);
                } catch (IOException e) {
                    log.debug("[store] remove lock file skipped (still in use): {}", lockFile);
                }
                jvmLock.unlock();
            }
        }
    }

    /**
     * 在「锁名」对应的跨进程临界区内执行：
     * <ol>
     *   <li>先取本 JVM 的 {@link ReentrantLock} —— 同一进程内（含多个编排器实例、多个线程）严格排队，
     *       也避免同一 JVM 对同一文件重复加 OS 锁触发 {@link OverlappingFileLockException}；</li>
     *   <li>再取锁文件上的 OS 文件锁（自旋等待其它 JVM 释放）——
     *       共享同一目录的不同进程之间严格串行；</li>
     *   <li>临界区结束后按相反顺序释放。进程崩溃时 OS 锁由内核自动回收，不会留下死锁。</li>
     * </ol>
     */
    private <T> T withFileLock(String jvmLockName, Path lockFile, Supplier<T> section) {
        try (CrossProcessLock ignored = new CrossProcessLock(jvmLockName, lockFile)) {
            return section.get();
        }
    }

    /**
     * 获取 OS 文件锁。{@code FileChannel.lock()} 对「同一 JVM 内其它通道已持有同一文件锁」会直接抛
     * {@link OverlappingFileLockException}（不阻塞），但这种情况已被外层 JVM 锁排除；
     * 对「其它 JVM 持有」则阻塞。这里统一用 {@code tryLock} + 短暂可中断自旋，
     * 保证本 JVM / 跨 JVM 的竞争者都会排队而不是报错；
     * 被中断（例如崩溃模拟里的「尸体」迟到线程）则立即快速失败，不拖死调用方。
     */
    private FileLock acquireOsLock(FileChannel channel, Path lockFile) throws IOException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (true) {
            FileLock osLock;
            try {
                osLock = channel.tryLock();
            } catch (OverlappingFileLockException sameJvm) {
                osLock = null; // 理论上被 JVM 内锁排除；防御性自旋
            }
            if (osLock != null) {
                return osLock;
            }
            if (System.nanoTime() > deadline) {
                throw new IOException("timed out waiting for cross-process lock: " + lockFile);
            }
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for cross-process lock: " + lockFile, e);
            }
        }
    }

    private static String safe(String raw) {
        return raw.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    @Override
    public Optional<SagaInstance> createIfAbsent(SagaInstance instance) {
        return withFileLock("inst:" + instance.getInstanceId(),
                instanceLockFile(instance.getInstanceId()), () -> {
            Path file = instanceFile(instance.getInstanceId());
            instance.setVersion(0);
            // 原子 create-if-absent（跨 JVM 安全）的两阶段发布：
            //   1) 内容完整写入唯一临时文件（发布前任何读者都看不到它）；
            //   2) 在与该实例对应的跨进程临界区内复查目标是否存在，不存在才原子 rename 发布。
            // 这样多个恢复者 / 多个进程共享同一目录时也只有唯一创建者，
            // 且读者要么看不到文件、要么只能读到完整 JSON，绝不会读到半截内容。
            if (Files.exists(file)) {
                log.debug("[store] createIfAbsent lost create race, instance exists {}",
                        instance.getInstanceId());
                return Optional.of(readInstance(file));
            }
            Path tmp = writeToTemp(file, instance);
            try {
                commitReplace(tmp, file);
                log.debug("[store] created new instance {} version=0", instance.getInstanceId());
                return Optional.<SagaInstance>empty();
            } catch (RuntimeException e) {
                deleteQuietly(tmp);
                throw e;
            }
        });
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
        withFileLock("inst:" + instance.getInstanceId(),
                instanceLockFile(instance.getInstanceId()), () -> {
            // 跨进程串行化「读版本 -> 比较 -> 写新内容」的 CAS 区间。
            // 多个恢复者（不同 JVM，或同一 JVM 的不同编排器）并发恢复同一实例时，
            // 败者仍会在本方法内收到 VERSION_CONFLICT，由编排器按「已被别人推进」安静收敛。
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
            return null;
        });
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

    @Override
    public <T> T withGuardLock(String guardKey, java.util.function.Supplier<T> section) {
        // 同一个 JVM 内多个编排器、以及共享同一目录的多个 JVM 之间，对同一守卫键都严格排队；
        // 先入者把守卫置为 IN_FLIGHT 并真正执行业务动作，后入者只会看到 DONE/IN_FLIGHT，
        // 不会有两个恢复者同时穿透同一个守卫动作。
        return withFileLock("guard:" + guardKey, guardLockFile(guardKey), section);
    }

    private SagaInstance readInstance(Path file) {
        try {
            return mapper.readValue(file.toFile(), SagaInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException("read instance failed: " + file, e);
        }
    }

    private void writeJson(Path target, Object value) {
        Path tmp = writeToTemp(target, value);
        commitReplace(tmp, target);
    }

    /**
     * 序列化到目标旁边的唯一临时文件（文件名带 UUID），不触碰目标。
     * 不同进程 / 线程并发写同一目标也不会撞临时文件、不会互相读到半截内容。
     */
    private Path writeToTemp(Path target, Object value) {
        Path tmp = uniqueTempFile(target);
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), value);
            return tmp;
        } catch (IOException e) {
            deleteQuietly(tmp);
            throw new UncheckedIOException("write json failed: " + target, e);
        }
    }

    /** 原子地用临时文件替换目标；不支持原子 move 的文件系统退回普通替换。 */
    private void commitReplace(Path tmp, Path target) {
        try {
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicFailure) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            deleteQuietly(tmp);
            throw new UncheckedIOException("commit json failed: " + target, e);
        }
    }

    private Path uniqueTempFile(Path target) {
        return target.resolveSibling(target.getFileName()
                + ".tmp." + ProcessHandle.current().pid() + "." + Thread.currentThread().getId()
                + "." + UUID.randomUUID());
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // best effort
        }
    }
}
