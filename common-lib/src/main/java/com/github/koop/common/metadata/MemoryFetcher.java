package com.github.koop.common.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryFetcher implements Fetcher {

    private final Map<Class<?>, Object> store = new ConcurrentHashMap<>();
    private ChangeListener<Object> listener;

    private MemoryFetcher(Object initial) {
        if (initial != null) {
            this.store.put(initial.getClass(), initial);
        }
    }

    public static MemoryFetcher start(Object initial, ChangeListener<Object> onChange) {
        MemoryFetcher fetcher = new MemoryFetcher(initial);
        fetcher.start(onChange);
        return fetcher;
    }

    @Override
    public void start(ChangeListener<Object> onChange) {
        this.listener = (ChangeListener<Object>) onChange;
    }

    @Override
    public <T> T fetchCurrent(Class<T> clazz) {
        return clazz.cast(store.get(clazz));
    }

    @Override
    public void close() {
        // No-op since this is a memory fetcher
    }

    public void update(Object newValue) {
        if (newValue != null) {
            var prev = this.store.put(newValue.getClass(), newValue);
            if (this.listener != null) {
                this.listener.onChange(prev, newValue);
            }
        }
    }
    
}
