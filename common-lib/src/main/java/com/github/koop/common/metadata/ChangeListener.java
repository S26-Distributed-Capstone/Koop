package com.github.koop.common.metadata;

public interface ChangeListener<T> {

    public void onChange(T prev, T current);
    
}
