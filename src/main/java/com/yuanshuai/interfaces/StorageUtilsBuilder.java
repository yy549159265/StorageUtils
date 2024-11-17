package com.yuanshuai.interfaces;

import com.yuanshuai.config.OscConfig;

@FunctionalInterface
public interface StorageUtilsBuilder<T, C> {
    T buildUtils(C client);
}
