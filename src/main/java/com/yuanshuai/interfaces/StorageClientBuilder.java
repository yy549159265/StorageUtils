package com.yuanshuai.interfaces;

import com.yuanshuai.config.OscConfig;

@FunctionalInterface
public interface StorageClientBuilder<T, C> {
    T buildClient(OscConfig config, C clientConfig);
}
