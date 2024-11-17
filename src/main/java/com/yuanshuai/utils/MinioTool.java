package com.yuanshuai.utils;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.yuanshuai.domain.minio.MinioFileInfo;
import com.yuanshuai.domain.minio.MinioListResult;
import io.minio.*;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MinioTool {

    private final MinioClient minioClient;

    public MinioTool(MinioClient minioClient) {
        this.minioClient = minioClient;
    }

    public void shutdown() {
        try {
            if (minioClient != null) {
                minioClient.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("无法关闭连接", e);
        }
    }

    /********************* 桶操作 **********/
    /**
     * 判断桶是否存在
     *
     * @param bucketName 桶名
     * @return true/false
     */
    public Boolean isBucketExists(String bucketName) {
        try {
            return minioClient.bucketExists(BucketExistsArgs.builder()
                    .bucket(bucketName)
                    .build());
        } catch (Exception e) {
            log.error("检查桶是否存在时发生错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 判断桶列表是否存在
     *
     * @param bucketNameList 桶名列表
     * @return true/false
     */
    public List<Boolean> isBucketExists(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(this::isBucketExists)
                .collect(Collectors.toList());
    }

    /**
     * 创建桶
     *
     * @param bucketName 桶名
     * @return true/false
     */
    public Boolean createBucket(String bucketName) {
        try {
            if (isBucketExists(bucketName)) {
                log.info("桶" + bucketName + "已存在");
                return false;
            } else {
                minioClient.makeBucket(MakeBucketArgs.builder()
                        .bucket(bucketName)
                        .build());
                log.info("创建桶" + bucketName + "成功");
            }
        } catch (Exception e) {
            log.error("创建桶时发生错误: " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 创建桶列表
     *
     * @param bucketNameList 桶名列表
     * @return true/false
     */
    public List<Boolean> createBucket(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(this::createBucket)
                .collect(Collectors.toList());
    }

    /**
     * 删除桶
     *
     * @param bucketName 桶名
     * @return true/false
     */
    public Boolean deleteBucket(String bucketName) {
        try {
            if (isBucketExists(bucketName)) {
                minioClient.removeBucket(RemoveBucketArgs.builder()
                        .bucket(bucketName)
                        .build());
                log.info("删除桶" + bucketName + "成功");
            } else {
                log.info("桶" + bucketName + "不存在");
            }
        } catch (Exception e) {
            log.error("删除桶时发生错误: " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 删除桶列表
     *
     * @param bucketNameList 桶名列表
     * @return true/false
     */
    public List<Boolean> deleteBucket(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(this::deleteBucket)
                .collect(Collectors.toList());
    }

    /**
     * 设置桶标签
     *
     * @param bucketName 桶名
     * @param tags       标签
     */
    public Boolean setBucketTags(String bucketName, Map<String, String> tags) {
        try {
            if (!isBucketExists(bucketName)) {
                log.info("桶" + bucketName + "不存在");
                return false;
            }
            minioClient.setBucketTags(SetBucketTagsArgs.builder()
                    .bucket(bucketName)
                    .tags(tags)
                    .build());
            log.info("设置桶" + bucketName + "标签成功");
        } catch (Exception e) {
            log.error("设置桶标签时发生错误: " + e.getMessage());
        }
        return true;
    }

    /**
     * 获取桶标签
     *
     * @param bucketName 桶名
     */
    public Map<String, String> getBucketTags(String bucketName) {
        try {
            return minioClient.getBucketTags(GetBucketTagsArgs.builder()
                    .bucket(bucketName)
                    .build()).get();
        } catch (Exception e) {
            log.error("获取桶标签时发生错误: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * 列出所有桶
     *
     * @return 桶名列表
     */
    public List<String> listBuckets() {
        try {
            return minioClient.listBuckets().stream()
                    .map(Bucket::name)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("列出桶时发生错误: " + e.getMessage());
            return Collections.emptyList();
        }

    }

    /********** 对象操作 **********/
    /**
     * 判断对象是否存在
     */
    public Boolean isObjectExists(String bucketName, String objectName) {
        try {
            // 获得对象的元数据。
            StatObjectArgs.Builder argsBuilder = StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName);
            minioClient.statObject(argsBuilder.build());
        } catch (Exception e) {
            log.error("获取对象失败: " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 查询单文件
     *
     * @param bucketName
     * @param objectName
     * @return ObjectStat 文件信息
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public StatObjectResponse getObject(String bucketName, String objectName) {
        try {
            // 获得对象的元数据。
            StatObjectArgs.Builder argsBuilder = StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName);
            StatObjectResponse objectStat = minioClient.statObject(argsBuilder.build());
            return objectStat;
        } catch (Exception e) {
            log.error("获取对象失败: " + e.getMessage());
        }
        return null;
    }

    /**
     * 查询文件列表
     *
     * @param bucketName
     * @param prefix     前缀
     * @param maxKeys    最大数量
     * @param recursive  是否递归
     * @return
     */
    public List<Item> listObjects(String bucketName, String prefix, int maxKeys, Boolean recursive) {
        try {
            if (!isBucketExists(bucketName)) {
                log.info("桶不存在，无法列出对象");
                return Collections.emptyList();
            }
            return fetchObjects(bucketName, prefix, maxKeys, recursive);
        } catch (Exception e) {
            log.error("列出所有对象时发生错误: " + e);
            return Collections.emptyList();
        }
    }
    public List<Item> listObjects(String bucketName, String prefix, int maxKeys) {
        return listObjects(bucketName, prefix, maxKeys, true);
    }
    public List<Item> listObjects(String bucketName, String prefix) {
        return listObjects(bucketName, prefix, 0, true);
    }
    public List<Item> listObjects(String bucketName) {
        return listObjects(bucketName, null, 0, true);
    }
    private List<Item> fetchObjects(String bucketName, String prefix, int maxKeys, Boolean recursive) throws Exception {
        List<Item> objectList = new ArrayList<>();
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder().bucket(bucketName);

            // 设置前缀、最大数量和递归选项
            if (prefix != null) {
                builder.prefix(prefix);
            }
            if (maxKeys > 0) {
                builder.maxKeys(maxKeys);
            }
            if (recursive) {
                builder.recursive(recursive);
            }
            builder.includeUserMetadata(true);

            // 列出对象
            Iterable<Result<Item>> results = minioClient.listObjects(builder.build());
            for (Result<Item> result : results) {
                objectList.add(result.get());
            }
        } catch (Exception e) {
            log.error("列出对象时发生错误: " + e.getMessage());
        }
        return objectList;
    }

    /**
     * 查询多个文件
     *
     * @param bucketName
     * @param objectNameList
     * @return
     */
    public List<StatObjectResponse> listObjects(String bucketName, List<String> objectNameList) {
        if (!isBucketExists(bucketName) || objectNameList.isEmpty()) {
            log.info("桶不存在或对象列表为空");
            return Collections.emptyList();
        }
        List<StatObjectResponse> collect = objectNameList.stream()
                .map(item -> getObject(bucketName, item))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * 删除单/多文件/某个目录下的所有文件，由于minio删除操作拿不到返回值，只能用Boolen表示是否成功
     *
     * @param bucketName
     * @param objectName
     * @return true/false
     */
    public Boolean deleteObject(String bucketName, String objectName) {
        try {
            if (isObjectExists(bucketName, objectName) == null) {
                log.info("对象不存在，无法删除");
                return false;
            }
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
            return true;
        } catch (Exception e) {
            log.error("删除对象失败: " + e.getMessage());
        }
        return false;
    }
    public Boolean deleteObject(String bucketName, List<String> objectNames) {
        if (!isBucketExists(bucketName) || CollectionUtil.isEmpty(objectNames)) {
            log.error("桶不存在或对象列表为空，无法删除");
            return false;
        }

        try {
            List<DeleteObject> deleteObjects = objectNames.stream().map(DeleteObject::new).collect(Collectors.toList());
            RemoveObjectsArgs build = RemoveObjectsArgs.builder()
                    .bucket(bucketName)
                    .objects(deleteObjects)
                    .build();

            Iterable<Result<DeleteError>> results = minioClient.removeObjects(build);
            for (Result<DeleteError> result : results) {
                DeleteError error = result.get();
                log.error("删除对象失败: " + error.objectName() + " - " + error.message());
            }
            return true; // 此处返回true，即使部分删除失败
        } catch (Exception e) {
            log.error("删除对象失败: " + e.getMessage(), e);
        }
        return false;
    }
    public Boolean deleteObjectDir(String bucketName, String dir) {
        try {
            if (!isBucketExists(bucketName)) {
                log.info("桶不存在，无法删除");
            }
            ListObjectsArgs.Builder argsBuilder = ListObjectsArgs.builder().bucket(bucketName).prefix(dir);
            Iterable<Result<Item>> results = minioClient.listObjects(argsBuilder.build());
            List<String> objectNames = new ArrayList<>();
            for (Result<Item> result : results) {
                Item item = result.get();
                objectNames.add(item.objectName());
            }
            if (CollectionUtil.isEmpty(objectNames)) {
                log.info("目录下无文件，无法删除");
                return false;
            }
            return deleteObject(bucketName, objectNames);
        } catch (Exception e) {
            log.error("删除目录失败: " + e.getMessage());
        }
        return false;
    }

    /**
     * 复制文件到指定桶，复制多文件到指定桶，复制桶（不可与源桶一致）
     * 单文件限制大小5g
     *
     * @param sourceBucketName 源桶名
     * @param sourceObjectName 源文件名
     * @param targetBucketName 目标桶名
     * @param targetObjectName 目标文件名
     * @return true/false
     */
    public Boolean copyObject(String sourceBucketName, String sourceObjectName, String targetBucketName, String targetObjectName) {
        try {
            if (isObjectExists(sourceBucketName, sourceObjectName) == null) {
                log.info("源对象不存在");
                return false;
            }
            if (!isBucketExists(targetBucketName)) {
                log.info("目标桶不存在");
                return false;
            }
            CopySource copySource = CopySource.builder().bucket(sourceBucketName).object(sourceObjectName).build();
            CopyObjectArgs copyObjectArgs = CopyObjectArgs.builder()
                    .source(copySource)
                    .bucket(targetBucketName)
                    .object(targetObjectName)
                    .build();
            minioClient.copyObject(copyObjectArgs);
            return true;
        } catch (Exception e) {
            log.error("复制对象失败: " + e.getMessage());
        }
        return false;
    }
    public List<MinioListResult> copyObjectList(String sourceBucketName, List<String> sourceObjectName, String targetBucketName) {
        List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
        sourceObjectName.forEach(item -> minioCopyObjectResultArrayList.add(new MinioListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
        return minioCopyObjectResultArrayList;
    }
    public List<MinioListResult> copyBucket(String sourceBucketName, String targetBucketName) {
        List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
        listObjects(sourceBucketName).stream()
                .map(Item::objectName)
                .forEach(item -> minioCopyObjectResultArrayList.add(new MinioListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
        return minioCopyObjectResultArrayList;
    }

    /**
     * 复制某个目录到其他目录，目录下所有文件都复制到目标目录下
     */
    public List<MinioListResult> copyObjectDir(String sourceBucketName, String sourceDir, String targetBucketName, String targetDir) {
        try {
            if (!isBucketExists(sourceBucketName)) {
                log.info("源桶不存在");
                return Collections.emptyList();
            }
            if (!isBucketExists(targetBucketName)) {
                log.info("目标桶不存在");
                return Collections.emptyList();
            }
            List<Item> items = listObjects(sourceBucketName, sourceDir);
            List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
            for (Item item : items) {
                String objectName = item.objectName();
                String targetObjectName = targetDir + objectName.substring(sourceDir.length());
                minioCopyObjectResultArrayList.add(new MinioListResult(objectName, copyObject(sourceBucketName, objectName, targetBucketName, targetObjectName)));
            }
            return minioCopyObjectResultArrayList;
        } catch (Exception e) {
            log.error("复制目录失败: " + e.getMessage());

        }
        return Collections.emptyList();
    }

    /********** 上传操作 **********/
    /**
     * 简单上传，MultipartFile类型上传单文件
     *
     * @param bucketName 桶名
     * @param objectName 对象名
     * @param file       为MultipartFile类型
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file,Map<String, String> userMetadata) {
        try (InputStream fis = file.getInputStream()) {
            String md5 = DigestUtil.md5Hex(fis);
            userMetadata.put("File-Md5", md5);
            PutObjectArgs build = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(file.getInputStream(), file.getSize(), -1)
                    .contentType(file.getContentType())
                    .userMetadata(userMetadata)
                    .build();
            minioClient.putObject(build);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file){
        return uploadFile(bucketName, objectName, file, new HashMap<>());
    }

    /**
     * 简单上传，InputStream类型上传单文件
     *
     * @param bucketName  桶名
     * @param objectName  对象名
     * @param inputStream 输入流
     * @param contentType 文件类型
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream, String contentType,Map<String, String> userMetadata) {
        try {
            String md5 = DigestUtil.md5Hex(inputStream);
            userMetadata.put("File-Md5", md5);
            PutObjectArgs build = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, inputStream.available(), -1)
                    .contentType(contentType)
                    .userMetadata(userMetadata)
                    .build();
            minioClient.putObject(build);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream) {
        return uploadFile(bucketName, objectName, inputStream, "application/octet-stream", new HashMap<>());
    }

    /**
     * 简单上传，本地文件类型上传单文件
     *
     * @param bucketName 桶名
     * @param objectName 对象名
     * @param filePath   文件路径
     * @param partSize   分片大小
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, String filePath, Long partSize,Map<String, String> userMetadata) {
        if (partSize < 5 * 1024 * 1024) {
            log.error("分片大小不能小于5MB");
            return false;
        }
        try (InputStream fis = Files.newInputStream(Paths.get(filePath))) {
            String md5 = DigestUtil.md5Hex(fis);
            userMetadata.put("File-Md5", md5);

            UploadObjectArgs build = UploadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .userMetadata(userMetadata)
                    .filename(filePath, partSize)
                    .build();
            minioClient.uploadObject(build);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String objectName, String bucketName, String filePath) {
        return uploadFile(objectName, bucketName, filePath, 5 * 1024 * 1024L, new HashMap<>());
    }

    /**
     * 简单上传，文件URL类型上传单文件
     *
     * @param bucketName 桶名
     * @param objectName 对象名
     * @param fileUrl    文件URL
     * @return true/false
     */
    public Boolean uploadUrlFile(String bucketName, String objectName, String fileUrl, Map<String, String> userMetadata, Boolean Md5) {
        try (InputStream uploadInputStream = new URL(fileUrl).openStream()) {
            // 获取文件大小
            HttpURLConnection connection = (HttpURLConnection) new URL(fileUrl).openConnection();
            connection.setRequestMethod("GET");
            long fileSize = connection.getContentLengthLong();

            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(uploadInputStream, fileSize, -1)
                    .contentType("application/octet-stream");
            if (Md5) {
                // 读取流会造成流的指针位置改变，导致无法获取流的MD5值，因此需要重新读取流
                InputStream md5InputStream = new URL(fileUrl).openStream();
                String md5 = DigestUtil.md5Hex(md5InputStream);
                userMetadata.put("File-Md5", md5);
                builder.userMetadata(userMetadata);
            }
            minioClient.putObject(builder.build());
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadUrlFile(String bucketName, String objectName, String fileUrl) {
        return uploadUrlFile(bucketName, objectName, fileUrl, new HashMap<>() ,true);
    }

    /**
     * 追加新内容上传，适用于问本，不计算md5
     *
     * @param bucketName
     * @param targetObjectName
     * @param file
     * @return true/false
     */
    public Boolean appendUpload(String bucketName, String targetObjectName, MultipartFile file) {
        try (InputStream inputStream = minioClient.getObject(GetObjectArgs.builder()
                .bucket(bucketName)
                .object(targetObjectName)
                .build());
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            // 读取现有对象内容并写入输出流
            byte[] buffer = new byte[1024];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, len);
            }

            // 追加新文件内容，分批次写入
            outputStream.write("\n".getBytes());
            try (InputStream fileInputStream = file.getInputStream()) {
                while ((len = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, len);
                }
            }

            // 使用新的内容直接上传到Minio，无需删除旧对象
            try (InputStream finalInputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                minioClient.putObject(PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(targetObjectName)
                        .stream(finalInputStream, finalInputStream.available(), -1)
                        .contentType(file.getContentType())
                        .build());
            }

            return true;
        } catch (Exception e) {
            log.error("追加上传文件时发生错误: " + e.getMessage(), e);
            return false;
        }
    }

    /**
     * 获取上传ID
     *
     * @return 上传ID 年月日/时分秒/uuid
     */
    public String getUploadId(String timezone) {
        cn.hutool.core.date.DateTime dateTime = DateUtil.convertTimeZone(DateUtil.date(), ZoneId.of(timezone));
        String datePart = DateUtil.format(dateTime, "yyyy-MM-dd");
        String timePart = DateUtil.format(dateTime, "HH-mm-ss");
        String uuid = IdUtil.fastUUID();
        return datePart + "/" + timePart + "/" + uuid;
    }
    public String getUploadId() {
        return getUploadId("Asia/Shanghai");
    }

    // 简单获取文件信息
    public MinioFileInfo createMinioFileInfo(String filePath, Long partSize, String bucketName, String objectName, String uploadId) {
        Path path = Paths.get(filePath);
        try (InputStream fis = Files.newInputStream(path)) {
            String fileMd5 = DigestUtil.md5Hex(fis);
            // 计算总分片数，并计算每个分片的MD5
            long fileSize = Files.size(path);
            long totalNum = fileSize / partSize + (fileSize % partSize > 0 ? 1 : 0);
            List<MinioFileInfo.PartInfo> parts = new ArrayList<>();
            for (int i = 1; i <= totalNum; i++) {
                long start = (i - 1) * partSize;
                long end = i * partSize - 1;
                if (i == totalNum) {
                    end = fileSize - 1;
                }
                // 计算bytes[]
                byte[] bytes = new byte[(int) (end - start + 1)];
                String partMd5 = DigestUtil.md5Hex(bytes);
                MinioFileInfo.PartInfo partInfo = new MinioFileInfo.PartInfo();
                partInfo.setCurrentNum(StrUtil.toString(i));
                partInfo.setPartMd5(partMd5);
                parts.add(partInfo);
            }

            MinioFileInfo minioFileInfo = new MinioFileInfo();
            minioFileInfo.setBucketName(bucketName);
            minioFileInfo.setObjectName(objectName);
            minioFileInfo.setUploadId(uploadId);
            minioFileInfo.setTotalNum(StrUtil.toString(totalNum));
            minioFileInfo.setFileMd5(fileMd5);
            minioFileInfo.setParts(parts);
            return minioFileInfo;
        } catch (Exception e) {
            log.error("计算分片MD5时发生错误: " + e.getMessage());
        }
        return null;
    }

    /**
     * 上传分片，仅支持分片小于5mb的分片
     *
     * @param tmpPath       分片上传的临时路径，若不指定，则默认在tmpFilePart/uploadId/下
     * @param minioFileInfo 文件详细信息
     * @param files         分片文件列表
     * @param Md5           是否开启md5校验，不开启为普通分片上传，开启为断点续传上传
     * @return true/false
     * minioFileInfo和file，需要使用RequestPart注解
     * minioFileInfo 需要content-type为application/json
     */
    public Boolean multipartUpload(String tmpPath, MinioFileInfo minioFileInfo, List<MultipartFile> files , Map<String, String> userMetadata, Boolean Md5) {
        List<MinioFileInfo.PartInfo> parts = minioFileInfo.getParts();
        if (CollectionUtil.isEmpty(parts) || CollectionUtil.isEmpty(files) || parts.size() != files.size()) {
            log.error("分片信息与分片文件数量不匹配");
            return false;
        }
        for (int i = 0; i < files.size(); i++) {
            try (InputStream inputStream = files.get(i).getInputStream()) {
                String tempPartName = tmpPath + "/" + minioFileInfo.getUploadId() + "/part-" + parts.get(i).getCurrentNum();
                if (Md5) {
                    String filePartMd5 = DigestUtil.md5Hex(inputStream);
                    String partMd5 = parts.get(i).getPartMd5();

                    // 检验分片MD5
                    if (!filePartMd5.equals(partMd5)) {
                        log.error("{}文件的分片{} MD5 异常，请检查", minioFileInfo.getObjectName(), parts.get(i).getCurrentNum());
                        return false;
                    }

                    // 检验分片是否已上传

                    if (isObjectExists(minioFileInfo.getBucketName(), tempPartName)) {
                        log.info("{}文件的分片{}已存在，无需上传", minioFileInfo.getObjectName(), parts.get(i).getCurrentNum());
                        continue;
                    }
                }
                log.info("开始上传 {} 文件的分片{}", minioFileInfo.getObjectName(), parts.get(i).getCurrentNum());
                userMetadata.put("Upload-Id", minioFileInfo.getUploadId());
                userMetadata.put("Total-Num", minioFileInfo.getTotalNum());
                userMetadata.put("Current-Num", parts.get(i).getCurrentNum());
                userMetadata.put("Part-Md5", parts.get(i).getPartMd5());

                PutObjectArgs builder = PutObjectArgs.builder()
                        .bucket(minioFileInfo.getBucketName())
                        .object(tempPartName)
                        .stream(files.get(i).getInputStream(), files.get(i).getSize(), -1)
                        .userMetadata(userMetadata)
                        .contentType(files.get(i).getContentType())
                        .build();

                minioClient.putObject(builder);
                log.info("分片{}上传成功", parts.get(i).getCurrentNum());
            } catch (Exception e) {
                log.error(StrUtil.format("{}文件的分片{}上传文件时发生错误: {}", minioFileInfo.getObjectName(), parts.get(i).getCurrentNum(), e.getMessage()));
                return false;
            }
        }
        log.info("{}文件的分片上传完成", minioFileInfo.getObjectName());
        return true;
    }
    public Boolean multipartUpload(MinioFileInfo minioFileInfo, List<MultipartFile> files) {
        return multipartUpload("tmpFilePart", minioFileInfo, files,new HashMap<>(), false);
    }
    public Boolean multipartUpload(MinioFileInfo minioFileInfo, List<MultipartFile> files, Boolean Md5) {
        return multipartUpload("tmpFilePart", minioFileInfo, files, new HashMap<>(),Md5);
    }

    /**
     * 合并分片上传
     *
     * @param tmpPath       分片上传的临时路径，若不指定，则默认在tmpFilePart/uploadId/下
     * @param minioFileInfo 文件详细信息
     * @param Md5           是否开启md5校验，不开启为普通分片上传，开启为断点续传上传
     *                      开启md5多一次api调用，但可以保证上传的完整性
     * @return
     */
    public Boolean composeMultipartUpload(String tmpPath, MinioFileInfo minioFileInfo, Map<String, String> userMetadata,Boolean Md5) {
        try {
            List<Item> items = listObjects(minioFileInfo.getBucketName(), tmpPath + "/" + minioFileInfo.getUploadId());

            // 检查目标对象是否已经存在
            if (Md5) {
                StatObjectResponse object = getObject(minioFileInfo.getBucketName(), minioFileInfo.getObjectName());
                String calculatedEtag = object.userMetadata().get("etag");
                if (object.etag().equals(calculatedEtag)) {
                    log.info("分片已存在且MD5匹配，无需合并");
                    return true; // 直接返回，表示无需再次合并,秒传
                }
            }

            // 查询分片对象
            if (Convert.toInt(minioFileInfo.getTotalNum()) != items.size()) {
                log.error(String.format("分片数量不一致，合并失败，提供分片数量：%s，查询到分片数量：%s", minioFileInfo.getTotalNum(), items.size()));
                return false;
            }
            List<String> partMd5List = minioFileInfo.getParts().stream()
                    .map(item -> item.getPartMd5())
                    .collect(Collectors.toList());
            String calculatedEtag = calculateETag(partMd5List, partMd5List.size());
            userMetadata.put("ETag", calculatedEtag);
            log.info("合并对象不存在，开始合并分片");

            // 构建ComposeObjectArgs
            List<ComposeSource> sources = items.stream()
                    .map(item -> ComposeSource.builder()
                            .bucket(minioFileInfo.getBucketName())
                            .object(item.objectName())
                            .build())
                    .collect(Collectors.toList());

            ComposeObjectArgs composeObjectArgs = ComposeObjectArgs.builder()
                    .bucket(minioFileInfo.getBucketName())
                    .object(minioFileInfo.getObjectName())
                    .sources(sources)
                    .userMetadata(userMetadata)
                    .build();
            // 合并分片
            minioClient.composeObject(composeObjectArgs);

            // 删除临时文件
            if (deleteObject(minioFileInfo.getBucketName(), items.stream().map(Item::objectName).collect(Collectors.toList()))) {
                log.info("合并分片成功，删除临时分片成功");
            }
            return true;
        } catch (Exception e) {
            log.error("合并分片上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean composeMultipartUpload(MinioFileInfo minioFileInfo, Boolean Md5) {
        return composeMultipartUpload("tmpFilePart", minioFileInfo, new HashMap<>(), Md5);
    }
    public Boolean composeMultipartUpload(MinioFileInfo minioFileInfo) {
        return composeMultipartUpload("tmpFilePart", minioFileInfo, new HashMap<>(), false);
    }

    /**
     * 计算ETag
     * 公式为ETag = md5(part1md5+part2md5+...)-n
     *
     * @param partMd5List 分片的md5列表
     * @param n           分片数量
     * @return ETag
     * 解释：Etag为文件内容的校验码，用于校验文件完整性
     */
    public String calculateETag(List<String> partMd5List, int n) {
        // 连接所有分片的MD5字节数组
        byte[] combinedMd5 = new byte[16 * n];
        for (int i = 0; i < n; i++) {
            byte[] partMd5 = HexUtil.decodeHex(partMd5List.get(i));
            System.arraycopy(partMd5, 0, combinedMd5, i * partMd5.length, partMd5.length);
        }

        // 计算合并后的MD5值
        byte[] finalMd5 = DigestUtil.md5(combinedMd5);

        // 转换为十六进制字符串
        String hexMd5 = HexUtil.encodeHexStr(finalMd5);

        // 最终的ETag格式
        return hexMd5 + "-" + n;
    }
    /****************************************/

    /********************* 下载操作 **********/
    /**
     * 简单下载，直接下载单文件
     *
     * @param objectName 文件名
     * @param bucketName 桶名
     * @param response   响应
     * @return true/false
     */
    public void downloadFile(String bucketName, String objectName, HttpServletResponse response) {
        try (InputStream inputStream = minioClient.getObject(GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build());
             OutputStream outputStream = response.getOutputStream()) {

            // 设置响应头
            response.setContentType("application/octet-stream");
            response.setHeader("Content-Disposition", "attachment; filename=\"" + objectName + "\"");

            // 将inputStream的数据复制到outputStream
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            outputStream.flush();
        } catch (Exception e) {
            log.error("下载文件时发生错误: " + e.getMessage(), e);
        }
    }

    /**
     * 下载文件到本地指定位置
     *
     * @param objectName 文件名
     * @param bucketName 桶名
     * @param filePath   本地路径，复杂路径需要用URLUtil.encode(filePath, CharsetUtil.CHARSET_UTF_8)处理才能当参数传递
     * @param override   是否覆盖本地文件
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public Boolean downloadFile(String filePath, String bucketName, String objectName, Boolean override) {
        try {
            DownloadObjectArgs build = DownloadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .filename(filePath)
                    .overwrite(override)
                    .build();
            minioClient.downloadObject(build);
            log.info("下载文件成功");
            return true;
        } catch (Exception e) {
            log.error("下载文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean downloadFile(String filePath, String bucketName,String objectName) {
        return downloadFile(filePath, bucketName, objectName, false);
    }

    /**
     * 断点下载，根据请求头的range下载
     * <img src="images/range.png"/>
     *
     * @param bucketName
     * @param objectName
     * @param request
     * @param response
     */
    public void checkpointDownload(String bucketName, String objectName, HttpServletRequest request, HttpServletResponse response) {
        long objectSize = 0;
        if (getObject(bucketName, objectName) != null) {
            objectSize = getObject(bucketName, objectName).size();
        } else {
            log.error("对象不存在: " + objectName);
        }
        String rangeHeader = request.getHeader("Range");
        try {
            long start = 0;
            long end = Long.MAX_VALUE;

            if (rangeHeader != null) {
                Pattern pattern = Pattern.compile("bytes=(\\d*)-(\\d*)");
                Matcher matcher = pattern.matcher(rangeHeader);
                if (matcher.matches()) {
                    String startPart = matcher.group(1);
                    String endPart = matcher.group(2);

                    if (!startPart.isEmpty()) {
                        start = Long.parseLong(startPart);
                    }
                    if (!endPart.isEmpty()) {
                        end = Long.parseLong(endPart);
                    }
                    if (end > objectSize - 1) {
                        end = objectSize - 1;
                    }
                }
                response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
            }

            response.setHeader("Content-Type", "application/octet-stream");
            response.setHeader("Accept-Ranges", "bytes");
            response.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + objectSize);

            try (InputStream inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .offset(start)
                            .length(end - start + 1)
                            .build());
                 OutputStream out = response.getOutputStream()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }
        } catch (Exception e) {
            log.error("下载文件时发生错误: " + e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
    /****************************************/


//    public List<MinioListResult> copyObjectList(String sourceBucketName, List<String> sourceObjectName, String targetBucketName) {
//        List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
//        sourceObjectName.forEach(item -> minioCopyObjectResultArrayList.add(new MinioListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
//        return minioCopyObjectResultArrayList;
//    }
//
//    public List<MinioListResult> copyBucket(String sourceBucketName, String targetBucketName) {
//        List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
//        listObjects(sourceBucketName).stream()
//                .map(MinioObjectStat::getObject)
//                .forEach(item -> minioCopyObjectResultArrayList.add(new MinioListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
//        return minioCopyObjectResultArrayList;
//    }
//
//    /**
//     * 复制某个目录到其他目录，目录下所有文件都复制到目标目录下
//     */
//    public List<MinioListResult> copyObjectDir(String sourceBucketName, String sourceDir, String targetBucketName, String targetDir) {
//        try {
//            if (!isBucketExists(sourceBucketName)) {
//                log.info("源桶不存在");
//                return Collections.emptyList();
//            }
//            if (!isBucketExists(targetBucketName)) {
//                log.info("目标桶不存在");
//                return Collections.emptyList();
//            }
//            List<MinioObjectStat> minioObjectStats = listObjects(sourceBucketName, sourceDir);
//            List<MinioListResult> minioCopyObjectResultArrayList = new ArrayList<>();
//            for (MinioObjectStat minioObjectStat : minioObjectStats) {
//                String objectName = minioObjectStat.getObject();
//                String targetObjectName = targetDir + objectName.substring(sourceDir.length());
//                minioCopyObjectResultArrayList.add(new MinioListResult(objectName, copyObject(sourceBucketName, objectName, targetBucketName, targetObjectName)));
//            }
//            return minioCopyObjectResultArrayList;
//        } catch (Exception e) {
//            log.error("复制目录失败: " + e.getMessage());
//
//        }
//        return Collections.emptyList();
//    }

}