package com.yuanshuai.interfaces;

@FunctionalInterface
public interface StorageUtilsBuilder<T, C> {
    T buildUtils(C client);
}
