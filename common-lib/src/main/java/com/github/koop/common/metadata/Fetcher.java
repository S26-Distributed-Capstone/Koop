package com.github.koop.common.metadata;

public interface Fetcher {
    void start(ChangeListener<Object> onChange);
    <T> T fetchCurrent(Class<T> clazz);
    void close();
}
