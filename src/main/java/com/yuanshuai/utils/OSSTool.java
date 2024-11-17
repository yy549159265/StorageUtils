package com.yuanshuai.utils;


import com.aliyun.oss.OSS;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OSSTool {

    private final OSS ossClient;

    public OSSTool(OSS ossClient) {
        this.ossClient = ossClient;

    }


    public void shutdown() {
        try {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        } catch (Exception e) {
            throw new RuntimeException("无法关闭连接",e);
        }
    }
}
