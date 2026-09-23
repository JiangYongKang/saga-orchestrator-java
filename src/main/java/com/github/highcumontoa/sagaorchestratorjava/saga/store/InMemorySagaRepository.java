package com.github.highcumontoa.sagaorchestratorjava.saga.store;

import com.github.highcumontoa.sagaorchestratorjava.saga.exception.SagaConcurrentConflictException;
import com.github.highcumontoa.sagaorchestratorjava.saga.model.SagaInstance;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 ConcurrentHashMap 的进程内存储（用于测试与演示）。
 *
 * <p>读与写均通过 JSON 序列化做深拷贝：调用方拿到的对象与存储内对象完全隔离，
 * 更新必须显式走 compareAndUpdate，杜绝绕过版本检查的就地修改。
 */
public class InMemorySagaRepository implements SagaRepository {

    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    @Override
    public boolean insertIfAbsent(SagaInstance instance) {
        String json = write(instance);
        return store.putIfAbsent(instance.getInstanceId(), json) == null;
    }

    @Override
    public Optional<SagaInstance> find(String instanceId) {
        String json = store.get(instanceId);
        return json == null ? Optional.empty() : Optional.of(read(json));
    }

    @Override
    public void compareAndUpdate(SagaInstance instance, long expectedVersion) {
        store.compute(instance.getInstanceId(), (id, currentJson) -> {
            if (currentJson == null) {
                throw new SagaConcurrentConflictException(id, expectedVersion, -1L);
            }
            long actualVersion = read(currentJson).getVersion();
            if (actualVersion != expectedVersion) {
                throw new SagaConcurrentConflictException(id, expectedVersion, actualVersion);
            }
            instance.setVersion(expectedVersion + 1);
            return write(instance);
        });
    }

    @Override
    public List<SagaInstance> findAll() {
        List<SagaInstance> all = new ArrayList<>();
        for (String json : store.values()) {
            all.add(read(json));
        }
        return all;
    }

    private static String write(SagaInstance instance) {
        try {
            return Json.MAPPER.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize saga instance", e);
        }
    }

    private static SagaInstance read(String json) {
        try {
            return Json.MAPPER.readValue(json, SagaInstance.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to deserialize saga instance", e);
        }
    }
}
