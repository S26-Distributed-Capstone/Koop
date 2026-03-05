package com.github.koop.common.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MemoryFetcher implements Fetcher {

    private final Map<Class<?>, Object> store = new ConcurrentHashMap<>();
    private Consumer<Object> listener;

    public MemoryFetcher() {
    }

    @Override
    public void start(Consumer<Object> onChange) {
        this.listener = onChange;
    }

    @Override
    public void close() {
        // No-op since this is a memory fetcher
    }

    public void update(Object newValue) {
        if (newValue != null) {
            this.store.put(newValue.getClass(), newValue);
            if (this.listener != null) {
                this.listener.accept(newValue);
            }
        }
    }
    
}
