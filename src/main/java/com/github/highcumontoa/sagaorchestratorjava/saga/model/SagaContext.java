package com.github.highcumontoa.sagaorchestratorjava.saga.model;

import java.util.HashMap;
import java.util.Map;

/** 步骤间共享的业务数据上下文，随实例持久化。 */
public class SagaContext {

    private Map<String, Object> values = new HashMap<>();

    public SagaContext() {
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) values.get(key);
    }

    public void put(String key, Object value) {
        values.put(key, value);
    }
}
