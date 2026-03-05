package com.github.koop.common.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MemoryFetcher implements Fetcher {

    private final Map<Class<?>, Object> store;
    private Consumer<Object> listener;

    private boolean started;
    private boolean closed;

    public MemoryFetcher() {
        this.store = new ConcurrentHashMap<>();
        this.started = false;
        this.closed = false;
    }

    @Override
    public void start(Consumer<Object> onChange) {
        this.listener = onChange;
        this.started = true;
    }

    @Override
    public void close() {
        // No-op since this is a memory fetcher
        this.closed = true;
    }

    public boolean wasStarted() {
        return started;
    }

    public boolean wasClosed() {
        return closed;
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
