package com.yuanshuai.interfaces;

import com.yuanshuai.config.StorageConfig;

@FunctionalInterface
public interface StorageClientBuilder<T, C> {
    T buildClient(StorageConfig config, C clientConfig);
}
