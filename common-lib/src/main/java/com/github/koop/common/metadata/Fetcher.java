package com.github.koop.common.metadata;

import java.util.function.Consumer;

public interface Fetcher {
    void start(Consumer<Object> onChange);
    void close();
}
