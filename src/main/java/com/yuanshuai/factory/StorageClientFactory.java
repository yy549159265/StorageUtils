package com.yuanshuai.factory;

import cn.hutool.core.util.StrUtil;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yuanshuai.config.OscConfig;
import com.yuanshuai.constants.StorageType;
import com.yuanshuai.interfaces.StorageClientBuilder;
import okhttp3.OkHttpClient;
import io.minio.MinioClient;
import org.apache.commons.io.IOUtils;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class StorageClientFactory {

    public static <T> T createClient(StorageType type, OscConfig config) {
        return createClient(type, config, null);
    }

    public static <T, C> T createClient(StorageType type, OscConfig config, C clientConfig) {
        validateConfig(config);
        switch (type) {
            case MINIO:
                return (T) minioBuilder.buildClient(config, (MinioClient.Builder) clientConfig);
            case GCS:
                return (T) gcsBuilder.buildClient(config, (String) clientConfig);
            case OBS:
                return (T) obsBuilder.buildClient(config, (ObsConfiguration) clientConfig);
            case OSS:
                return (T) ossBuilder.buildClient(config, (ClientBuilderConfiguration) clientConfig);
            case S3:
                return (T) s3Builder.buildClient(config, (S3ClientBuilder) clientConfig);
            default:
                throw new IllegalArgumentException("不支持的存储类型: " + type);
        }
    }


    private static void validateConfig(OscConfig config) {
        if (StrUtil.isBlank(config.getEndpoint())) {
            throw new IllegalArgumentException("端点不能为空");
        }
        if (StrUtil.isBlank(config.getAccessKey())) {
            throw new IllegalArgumentException("访问密钥不能为空");
        }
        if (StrUtil.isBlank(config.getSecretKey())) {
            throw new IllegalArgumentException("密钥不能为空");
        }
        if (StrUtil.isBlank(config.getBucketName())) {
            throw new IllegalArgumentException("存储桶名称不能为空");
        }
        if (StrUtil.isBlank(config.getRegion())) {
            throw new IllegalArgumentException("区域不能为空");
        }
    }
    private static final StorageClientBuilder<MinioClient, MinioClient.Builder> minioBuilder = (config, clientConfig) -> {
        try {
            MinioClient.Builder builder = (clientConfig != null) ? clientConfig : MinioClient.builder();
            return builder
                    .endpoint(config.getEndpoint())
                    .credentials(config.getAccessKey(), config.getSecretKey())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("创建Minio客户端失败", e);
        }
    };

    private static final StorageClientBuilder<Storage, String> gcsBuilder = (config, clientConfig) -> {
        if (clientConfig == null) {
            throw new IllegalArgumentException("必须指定谷歌云存储客户端的json文件路径，如果不指定请用s3代替");
        }
        try (FileInputStream fileInputStream = new FileInputStream(clientConfig)) {
            GoogleCredentials credentials = GoogleCredentials.fromStream(fileInputStream);
            return StorageOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();
        } catch (IOException e) {
            throw new RuntimeException("创建谷歌云存储客户端失败", e);
        }
    };

    private static final StorageClientBuilder<ObsClient, ObsConfiguration> obsBuilder = (config, clientConfig) -> {
        try {
            ObsConfiguration obsConfiguration = (clientConfig != null) ? clientConfig : new ObsConfiguration();
            obsConfiguration.setEndPoint(config.getEndpoint());
            return new ObsClient(config.getAccessKey(), config.getSecretKey(), obsConfiguration);
        } catch (Exception e) {
            throw new RuntimeException("创建OBS客户端失败", e);
        }
    };

    private static final StorageClientBuilder<OSS, ClientBuilderConfiguration> ossBuilder = (config, clientConfig) -> {
        try {
            DefaultCredentialProvider defaultCredentialProvider = new DefaultCredentialProvider(config.getAccessKey(), config.getSecretKey());
            ClientBuilderConfiguration configuration = (clientConfig != null) ? clientConfig : new ClientBuilderConfiguration();
            return OSSClientBuilder.create()
                    .endpoint(config.getEndpoint())
                    .credentialsProvider(defaultCredentialProvider)
                    .clientConfiguration(configuration)
                    .region(config.getRegion())
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("创建OSS客户端失败", e);
        }
    };


    private static final StorageClientBuilder<S3Client, S3ClientBuilder> s3Builder = (config, clientConfig) -> {
        try {
            Region region = Region.of(config.getRegion());
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
            URI endpointuri = URI.create(config.getEndpoint());
            S3ClientBuilder builder = (clientConfig != null) ? clientConfig : S3Client.builder();
            return builder
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .endpointOverride(endpointuri)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("创建S3客户端失败", e);
        }
    };


    public static void main(String[] args) {
        MinioClient client = StorageClientFactory.createClient(StorageType.MINIO, new OscConfig());
        Storage client1 = StorageClientFactory.createClient(StorageType.GCS, new OscConfig());
        ObsClient client2 = StorageClientFactory.createClient(StorageType.OBS, new OscConfig());
        OSS client3 = StorageClientFactory.createClient(StorageType.OSS, new OscConfig());
        S3Client client4 = StorageClientFactory.createClient(StorageType.S3, new OscConfig());

    }

}


