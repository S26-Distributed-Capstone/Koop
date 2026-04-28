package com.github.koop.common.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataClient implements AutoCloseable {
    private final Fetcher fetcher;
    private final Map<Class<?>, Object> cache = new ConcurrentHashMap<>();
    private final Map<Class<?>, List<ChangeListener<?>>> listeners = new ConcurrentHashMap<>();

    private final static Logger logger = LogManager.getLogger(MetadataClient.class);
    private boolean started = false;
    private boolean closed = false;
    public MetadataClient(Fetcher fetcher) {
        this.fetcher = fetcher;
    }

    public <T> void listen(Class<T> clazz, ChangeListener<T> listener) {
        this.listeners.compute(clazz, (k, lst) -> {
            if (lst == null) {
                lst = new CopyOnWriteArrayList<>();
            }
            lst.add(listener);
            return lst;
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void start() {
        if(started) {
            throw new IllegalStateException("MetadataClient has already been started");
        }
        if(closed) {
            throw new IllegalStateException("MetadataClient has already been closed");
        }
        this.fetcher.start(newObj->{
            var clazz = newObj.getClass();
            var prev = cache.put(clazz, newObj);
            var listeners = this.listeners.get(clazz);
            if (listeners != null) {
                for(ChangeListener listener:listeners){
                    try {
                        listener.onChange(prev, newObj);
                    } catch (Exception e) {
                        logger.error("Error in metadata listener for obj: {}, err: {}",clazz, e.getMessage());
                    }
                }
            }
            
        });
        started = true;
    }

    public <T> T get(Class<T> clazz) {
        return clazz.cast(cache.get(clazz));
    }

    /**
     * Blocks until a value for {@code clazz} is available in the cache, or until
     * the timeout elapses. Returns the cached value (possibly {@code null} if the
     * timeout expired before any value arrived).
     */
    public <T> T waitFor(Class<T> clazz, long timeout, TimeUnit unit) throws InterruptedException {
        T val = get(clazz);
        if (val != null) return val;
        CountDownLatch latch = new CountDownLatch(1);
        listen(clazz, (prev, curr) -> latch.countDown());
        val = get(clazz); // re-check after listener registered to close the race
        if (val != null) return val;
        latch.await(timeout, unit);
        return get(clazz);
    }

    @Override
    public void close() throws Exception {
        if(closed) {
            throw new IllegalStateException("MetadataClient has already been closed");
        }
        closed = true;
        fetcher.close();
    }

    public boolean isStarted() {
        return started;
    }
    public boolean isClosed() {
        return closed;
    }
    
}
