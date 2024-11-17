package com.yuanshuai.utils;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

@Slf4j
public class S3Tool {

    private final S3Client s3Client;

    public S3Tool(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void shutdown(){
        try {
            if (s3Client != null) {
                s3Client.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("无法关闭连接",e);
        }
    }
}
