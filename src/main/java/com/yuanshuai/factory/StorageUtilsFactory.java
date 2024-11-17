package com.yuanshuai.factory;

import com.aliyun.oss.OSS;
import com.google.cloud.storage.Storage;
import com.obs.services.ObsClient;
import com.yuanshuai.constants.StorageType;
import com.yuanshuai.interfaces.StorageUtilsBuilder;
import com.yuanshuai.utils.*;
import io.minio.MinioClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Map;

public class StorageUtilsFactory {


    // 存储每个存储类型的构建器
    private static final Map<StorageType, StorageUtilsBuilder<?, ?>> builders = new HashMap<>();

    static {
        // 注册 Minio 工具构建器
        builders.put(StorageType.MINIO, (StorageUtilsBuilder<MinioTool, MinioClient>) MinioTool::new);

        // 注册 GCS 工具构建器
        builders.put(StorageType.GCS, (StorageUtilsBuilder<GCSTool, Storage>) GCSTool::new);

        // 注册 OBS 工具构建器
        builders.put(StorageType.OBS, (StorageUtilsBuilder<OBSTool, ObsClient>) OBSTool::new);

        // 注册 OSS 工具构建器
        builders.put(StorageType.OSS, (StorageUtilsBuilder<OSSTool, OSS>) OSSTool::new);

        // 注册 S3 工具构建器
        builders.put(StorageType.S3, (StorageUtilsBuilder<S3Tool, S3Client>) S3Tool::new);
    }


    public static <T, C> T createUtils(StorageType type, C client) {
        StorageUtilsBuilder<T, C> builder = (StorageUtilsBuilder<T, C>) builders.get(type);
        if (builder == null) {
            throw new IllegalArgumentException("不支持的存储类型: " + type);
        }
        return builder.buildUtils(client);
    }
}
