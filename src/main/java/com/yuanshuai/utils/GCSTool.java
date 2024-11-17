package com.yuanshuai.utils;

import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GCSTool {

    private final Storage storage;

    public GCSTool(Storage storage) {
        this.storage = storage;
    }


    public void shutdown() {
            try {
                if (storage != null) {
                storage.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("无法关闭连接",e);
            }
    }
}
