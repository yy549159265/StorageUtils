package com.yuanshuai.utils;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.digest.DigestUtil;
import com.aliyun.oss.model.PartSummary;
import com.yuanshuai.domain.FileInfo;
import com.yuanshuai.domain.ListResult;
import io.minio.*;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.utils.Md5Utils;

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


    /********************* 桶操作 **********/
    /**
     * 判断桶是否存在
     *
     * @param bucketName 桶名
     * @return true/false
     */
    public Boolean isBucketExists(String bucketName) {
        try {
            HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.headBucket(headBucketRequest);
            return true;
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
    public List<ListResult> isBucketExists(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(item -> new ListResult(item, isBucketExists(item)))
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
                s3Client.createBucket(CreateBucketRequest.builder()
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
    public List<ListResult> createBucket(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(item -> new ListResult(item, createBucket(item)))
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
                s3Client.deleteBucket(DeleteBucketRequest.builder()
                        .bucket(bucketName)
                        .build());
                log.info("删除桶" + bucketName + "成功");
                return true;
            } else {
                log.info("桶" + bucketName + "不存在");
                return false;
            }
        } catch (Exception e) {
            log.error("删除桶时发生错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除桶列表
     *
     * @param bucketNameList 桶名列表
     * @return true/false
     */
    public List<ListResult> deleteBucket(List<String> bucketNameList) {
        return bucketNameList.stream()
                .map(item -> new ListResult(item, deleteBucket(item)))
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
            if (tags == null || tags.isEmpty()){
                log.info("标签为空，不设置标签");
                return false;
            }
            List<Tag> tagsList = new ArrayList<>();
            tags.forEach((k, v) -> {
                Tag tag = Tag.builder().key(k).value(v).build();
                tagsList.add(tag);
            });
            Tagging tagging = Tagging.builder().tagSet(tagsList).build();
            s3Client.putBucketTagging(PutBucketTaggingRequest.builder()
                    .bucket(bucketName)
                    .tagging(tagging)
                    .build());
            log.info("设置桶" + bucketName + "标签成功");
            return true;
        } catch (Exception e) {
            log.error("设置桶标签时发生错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 获取桶标签
     *
     * @param bucketName 桶名
     */
    public Map<String, String> getBucketTags(String bucketName) {
        try {
            return s3Client.getBucketTagging(GetBucketTaggingRequest.builder()
                    .bucket(bucketName)
                    .build())
                    .tagSet()
                    .stream()
                    .collect(Collectors.toMap(Tag::key, Tag::value));
        } catch (Exception e) {
            log.error("获取桶标签时发生错误: " + e.getMessage());
            return Collections.singletonMap("获取桶标签时发生错误", e.getMessage());
        }
    }

    /**
     * 删除桶标签
     *
     * @param bucketName 桶名
     */
    public Boolean deleteBucketTags(String bucketName) {
        try {
            s3Client.deleteBucketTagging(DeleteBucketTaggingRequest.builder()
                    .bucket(bucketName)
                    .build());
            return true;
        } catch (Exception e) {
            log.error("获取桶标签时发生错误: " + e.getMessage());
            return false;
        }
    }

    /**
     * 列出所有桶
     *
     * @return 桶名列表
     */
    public List<String> listBuckets() {
        try {
            return s3Client.listBuckets().buckets().stream()
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
            s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectName).build());
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
    public GetObjectResponse getObject(String bucketName, String objectName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();
            ResponseInputStream<GetObjectResponse> object = s3Client.getObject(getObjectRequest);
            log.info("获取对象成功 {}",object.response().eTag());
            return object.response();
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
     * @return
     */
    public List<S3Object> listObjects(String bucketName, String prefix, int maxKeys) {
        try {
            if (!isBucketExists(bucketName)) {
                log.info("桶不存在，无法列出对象");
                return Collections.emptyList();
            }
            return fetchObjects(bucketName, prefix, maxKeys);
        } catch (Exception e) {
            log.error("列出所有对象时发生错误: " + e);
            return Collections.emptyList();
        }
    }
    public List<S3Object> listObjects(String bucketName, String prefix) {
        return listObjects(bucketName, prefix, 0);
    }
    public List<S3Object> listObjects(String bucketName) {
        return listObjects(bucketName, null, 0);
    }
    private List<S3Object> fetchObjects(String bucketName, String prefix, int maxKeys) {
        try {
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucketName);
            // 设置前缀、最大数量和递归选项
            if (prefix != null) {
                builder.prefix(prefix);
            }
            if (maxKeys > 0) {
                builder.maxKeys(maxKeys);
            }
            ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(builder.build());
            return listObjectsV2Response.contents();
        } catch (Exception e) {
            log.error("列出对象时发生错误: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 查询多个文件
     *
     * @param bucketName
     * @param objectNameList
     * @return
     */
    public List<GetObjectResponse> listObjects(String bucketName, List<String> objectNameList) {
        if (!isBucketExists(bucketName) || objectNameList.isEmpty()) {
            log.info("桶不存在或对象列表为空");
            return Collections.emptyList();
        }
        List<GetObjectResponse> collect = objectNameList.stream()
                .map(item -> getObject(bucketName, item))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * 删除单/多文件/某个目录下的所有文件，由于s3删除操作拿不到返回值，只能用Boolen表示是否成功
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
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
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
            List<ObjectIdentifier> objectIdentifiers = objectNames.stream().map(item -> ObjectIdentifier.builder().key(item).build()).collect(Collectors.toList());
            Delete delete = Delete.builder().objects(objectIdentifiers).build();
            DeleteObjectsRequest build = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(delete)
                    .build();
            s3Client.deleteObjects(build);
            return true; // 此处返回true，即使部分删除失败
        } catch (Exception e) {
            log.error("删除对象失败: " + e.getMessage(), e);
            return false;
        }

    }
    public Boolean deleteObjectDir(String bucketName, String dir) {
        try {
            if (!isBucketExists(bucketName)) {
                log.info("桶不存在，无法删除");
            }
            List<S3Object> s3Objects = listObjects(bucketName, dir);
            List<String> objectNames = new ArrayList<>();
            for (S3Object s3Object : s3Objects) {
                String key = s3Object.key();
                objectNames.add(key);
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
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucketName)
                    .sourceKey(sourceObjectName)
                    .destinationBucket(targetBucketName)
                    .destinationKey(targetObjectName)
                    .build();
            s3Client.copyObject(copyObjectRequest);
            return true;
        } catch (Exception e) {
            log.error("复制对象失败: " + e.getMessage());
            return false;
        }

    }
    public List<ListResult> copyObjectList(String sourceBucketName, List<String> sourceObjectName, String targetBucketName) {
        List<ListResult> resultList = new ArrayList<>();
        sourceObjectName.forEach(item -> resultList.add(new ListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
        return resultList;
    }
    public List<ListResult> copyBucket(String sourceBucketName, String targetBucketName) {
        List<ListResult> resultList = new ArrayList<>();
        listObjects(sourceBucketName).stream()
                .map(S3Object::key)
                .forEach(item -> resultList.add(new ListResult(item, copyObject(sourceBucketName, item, targetBucketName, item))));
        return resultList;
    }

    /**
     * 复制某个目录到其他目录，目录下所有文件都复制到目标目录下
     */
    public List<ListResult> copyObjectDir(String sourceBucketName, String sourceDir, String targetBucketName, String targetDir) {
        try {
            if (!isBucketExists(sourceBucketName)) {
                log.info("源桶不存在");
                return Collections.emptyList();
            }
            if (!isBucketExists(targetBucketName)) {
                log.info("目标桶不存在");
                return Collections.emptyList();
            }
            List<S3Object> s3Objects = listObjects(sourceBucketName, sourceDir);
            List<ListResult> resultList = new ArrayList<>();
            for (S3Object s3Object : s3Objects) {
                String objectName = s3Object.key();
                String targetObjectName = targetDir + objectName.substring(sourceDir.length());
                resultList.add(new ListResult(objectName, copyObject(sourceBucketName, objectName, targetBucketName, targetObjectName)));
            }
            return resultList;
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
     * @param userMetadata 用户自定义元数据
     * @param Md5         是否校验MD5
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file, Map<String, String> userMetadata, Boolean Md5) {
        try (InputStream fis = file.getInputStream()) {
            String realMd5 = DigestUtil.md5Hex(fis);

            userMetadata.put("File-Md5", realMd5);
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .contentType(file.getContentType())
                    .metadata(userMetadata);
            if (Md5) {
                // 将16进制md5转换base64md5
                byte[] bytes = HexUtil.decodeHex(realMd5);
                String base64Md5 = Base64.encode(bytes);
                builder.contentMD5(base64Md5);
            }
            RequestBody requestBody = RequestBody.fromInputStream(file.getInputStream(), file.getSize());
            PutObjectResponse putObjectResponse = s3Client.putObject(builder.build(), requestBody);
            log.info("文件etag: {}",putObjectResponse.eTag());
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file, Map<String, String> userMetadata){
        return uploadFile(bucketName, objectName, file, userMetadata,false);
    }
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file){
        return uploadFile(bucketName, objectName, file, new HashMap<>(),false);
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
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream, String contentType,Map<String, String> userMetadata, Boolean Md5) {
        try {
            String realMd5 = DigestUtil.md5Hex(inputStream);
            userMetadata.put("File-Md5", realMd5);
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .contentType(contentType)
                    .metadata(userMetadata);
            if (Md5) {
                // 将16进制md5转换base64md5
                byte[] bytes = HexUtil.decodeHex(realMd5);
                String base64Md5 = Base64.encode(bytes);
                builder.contentMD5(base64Md5);
            }
            RequestBody requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
            s3Client.putObject(builder.build(), requestBody);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream) {
        return uploadFile(bucketName, objectName, inputStream, "application/octet-stream", new HashMap<>(),false);
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
    public Boolean uploadFile(String bucketName, String objectName, String filePath, Long partSize,Map<String, String> userMetadata, Boolean Md5) {
        if (partSize < 5 * 1024 * 1024) {
            log.error("分片大小不能小于5MB");
            return false;
        }
        Path path = Paths.get(filePath);
        try (InputStream fis = Files.newInputStream(path)) {
            String realMd5 = DigestUtil.md5Hex(fis);
            userMetadata.put("File-Md5", realMd5);
            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .metadata(userMetadata);
            if (Md5) {
                // 将16进制md5转换base64md5
                byte[] bytes = HexUtil.decodeHex(realMd5);
                String base64Md5 = Base64.encode(bytes);
                builder.contentMD5(base64Md5);
            }
            s3Client.putObject(builder.build() , path);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String objectName, String bucketName, String filePath) {
        return uploadFile(objectName, bucketName, filePath, 5 * 1024 * 1024L, new HashMap<>(),false);
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

            // 读取流会造成流的指针位置改变，导致无法获取流的MD5值，因此需要重新读取流
            InputStream md5InputStream = new URL(fileUrl).openStream();
            String realMd5 = DigestUtil.md5Hex(md5InputStream);
            userMetadata.put("File-Md5", realMd5);

            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .contentType("application/octet-stream")
                    .metadata(userMetadata);

            if (Md5) {
                // 将16进制md5转换base64md5
                byte[] bytes = HexUtil.decodeHex(realMd5);
                String base64Md5 = Base64.encode(bytes);
                builder.contentMD5(base64Md5);
            }
            s3Client.putObject(builder.build(), RequestBody.fromInputStream(uploadInputStream, fileSize));
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadUrlFile(String bucketName, String objectName, String fileUrl) {
        return uploadUrlFile(bucketName, objectName, fileUrl, new HashMap<>(),false);
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
        try {

            // 获取现有对象内容
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(targetObjectName)
                    .build();
            ResponseBytes<GetObjectResponse> object = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes());
            byte[] existingContent = object.asByteArray();

            // 将现有内容和新内容组合起来
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(existingContent);
            outputStream.write("\n".getBytes());

            // 读取文件内容并追加
            try (InputStream fileInputStream = file.getInputStream()) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, len);
                }
            }

            // 使用新的内容上传到S3
            byte[] newContent = outputStream.toByteArray();
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(targetObjectName)
                    .contentType(file.getContentType())
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(newContent));

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
    public Map<String, String>  getUploadId(String bucketName, String objectName, String timezone) {
        CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(objectName)
                .build());
        String uploadId = multipartUpload.uploadId();
        DateTime dateTime = DateUtil.convertTimeZone(DateUtil.date(), ZoneId.of(timezone));
        String datePart = DateUtil.format(dateTime, "yyyy-MM-dd");
        String timePart = DateUtil.format(dateTime, "HH-mm-ss");
        String uploadUrl = datePart + "/" + timePart + "/" + uploadId;
        Map<String, String> uploadIdMap = new HashMap<>();
        uploadIdMap.put("uploadId", uploadId);
        uploadIdMap.put("uploadUrl", uploadUrl);
        return uploadIdMap;
    }
    public Map<String, String> getUploadId(String bucketName, String objectName) {
        return getUploadId(bucketName, objectName, "Asia/Shanghai");
    }

    // 简单获取文件信息
    public FileInfo createMinioFileInfo(String filePath, Long partSize, String bucketName, String objectName, String uploadUrl) {
        Path path = Paths.get(filePath);
        try (InputStream fis = Files.newInputStream(path)) {
            String fileMd5 = DigestUtil.md5Hex(fis);
            // 计算总分片数，并计算每个分片的MD5
            long fileSize = Files.size(path);
            long totalNum = fileSize / partSize + (fileSize % partSize > 0 ? 1 : 0);
            List<FileInfo.PartInfo> parts = new ArrayList<>();
            for (int i = 1; i <= totalNum; i++) {
                long start = (i - 1) * partSize;
                long end = i * partSize - 1;
                if (i == totalNum) {
                    end = fileSize - 1;
                }
                // 计算bytes[]
                byte[] bytes = new byte[(int) (end - start + 1)];
                String partMd5 = DigestUtil.md5Hex(bytes);
                FileInfo.PartInfo partInfo = new FileInfo.PartInfo();
                partInfo.setCurrentNum(StrUtil.toString(i));
                partInfo.setPartMd5(partMd5);
                parts.add(partInfo);
            }
            String uploadId = uploadUrl.substring(uploadUrl.lastIndexOf("/") + 1);
            FileInfo s3FileInfo = new FileInfo();
            s3FileInfo.setBucketName(bucketName);
            s3FileInfo.setObjectName(objectName);
            s3FileInfo.setUploadId(uploadId);
            s3FileInfo.setUploadUrl(uploadUrl);
            s3FileInfo.setTotalNum(StrUtil.toString(totalNum));
            s3FileInfo.setFileMd5(fileMd5);
            s3FileInfo.setParts(parts);
            return s3FileInfo;
        } catch (Exception e) {
            log.error("计算分片MD5时发生错误: " + e.getMessage());
        }
        return null;
    }

    /**
     * 上传分片，仅支持分片小于5mb的分片
     *
     * @param s3FileInfo 文件详细信息
     * @param files         分片文件列表
     * @param Md5           是否开启md5校验，不开启为普通分片上传，开启为断点续传上传
     * @return true/false
     * s3FileInfo和file，需要使用RequestPart注解
     * s3FileInfo 需要content-type为application/json
     */
    public Boolean multipartUpload(FileInfo s3FileInfo, List<MultipartFile> files, Boolean Md5) {
        List<FileInfo.PartInfo> parts = s3FileInfo.getParts();
        if (CollectionUtil.isEmpty(parts) || CollectionUtil.isEmpty(files) || parts.size() != files.size()) {
            log.error("分片信息与分片文件数量不匹配");
            return false;
        }
        Set<Integer> uploadedPartNumbers = new HashSet<>();
        if (Md5) {
            // 1. 获取已上传的分片信息
            List<Part> uploadedParts = s3Client.listParts(ListPartsRequest.builder()
                    .bucket(s3FileInfo.getBucketName())
                    .key(s3FileInfo.getObjectName())
                    .uploadId(s3FileInfo.getUploadId())
                    .build()).parts();

            // 2. 确定哪些分片已经上传
            for (Part part : uploadedParts) {
                uploadedPartNumbers.add(part.partNumber());
            }
        }
        // 3. 从未上传的分片开始上传
        for (int i = 0; i < files.size(); i++) {
            if (uploadedPartNumbers.contains(i + 1)) {
                // 已上传，跳过
                log.info("{}文件的分片{}已经上传，跳过", s3FileInfo.getObjectName(), parts.get(i).getCurrentNum());
                continue;
            }
            try (InputStream inputStream = files.get(i).getInputStream()) {
                // 上传当前分片
                log.info("开始上传 {} 文件的分片{}", s3FileInfo.getObjectName(), parts.get(i).getCurrentNum());
                UploadPartRequest build = UploadPartRequest.builder()
                        .bucket(s3FileInfo.getBucketName())
                        .key(s3FileInfo.getObjectName())
                        .uploadId(s3FileInfo.getUploadId())
                        .partNumber(i + 1)
                        .build();

                s3Client.uploadPart(build, RequestBody.fromInputStream(inputStream, files.get(i).getSize()));
                log.info("分片{}上传成功", parts.get(i).getCurrentNum());
            } catch (Exception e) {
                log.error(StrUtil.format("{}文件的分片{}上传文件时发生错误: {}", s3FileInfo.getObjectName(), parts.get(i).getCurrentNum(), e.getMessage()));
                return false;
            }
        }
        log.info("{}文件的分片上传完成", s3FileInfo.getObjectName());
        return true;
    }
    public Boolean multipartUpload(FileInfo s3FileInfo, List<MultipartFile> files) {
        return multipartUpload(s3FileInfo, files, false);
    }

    /**
     * 合并分片上传
     *
     * @param s3FileInfo    文件详细信息
     * @param Md5           是否开启md5校验，不开启为普通分片合并，开启可启动秒传
     *                      开启md5多一次api调用，但可以保证上传的完整性
     * @return
     */
    public Boolean composeMultipartUpload(FileInfo s3FileInfo,Boolean Md5) {
        try {
            // 检查目标对象是否已经存在
            if (Md5 && isObjectExists(s3FileInfo.getBucketName(), s3FileInfo.getObjectName())) {
                GetObjectResponse object = getObject(s3FileInfo.getBucketName(), s3FileInfo.getObjectName());
                List<String> partMd5List = s3FileInfo.getParts()
                        .stream()
                        .map(FileInfo.PartInfo::getPartMd5)
                        .collect(Collectors.toList());
                String calculateETag = calculateETag(partMd5List, partMd5List.size());
                String cleanETag = object.eTag().replace("\"", "");
                if (calculateETag.equals(cleanETag)) {
                    log.info("文件已存在且MD5匹配，无需合并");
                    return true; // 直接返回，表示无需再次合并,秒传
                }
            }
            log.info("合并对象不存在，开始合并分片");
            // 构建CompletedPart
            List<CompletedPart> completedParts = new ArrayList<>();
            List<FileInfo.PartInfo> parts = s3FileInfo.getParts();
            for (int i = 0; i < parts.size(); i++) {
                completedParts.add(CompletedPart.builder().eTag(parts.get(i).getPartMd5()).partNumber(i + 1).build());
            }
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
            // 合并分片
            CompleteMultipartUploadRequest build = CompleteMultipartUploadRequest.builder()
                    .bucket(s3FileInfo.getBucketName())
                    .key(s3FileInfo.getObjectName())
                    .multipartUpload(completedMultipartUpload)
                    .uploadId(s3FileInfo.getUploadId())
                    .build();
            s3Client.completeMultipartUpload(build);
            return true;
        } catch (Exception e) {
            log.error("合并分片上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean composeMultipartUpload(FileInfo s3FileInfo) {
        return composeMultipartUpload(s3FileInfo, false);
    }

    /** ：列出存储桶中所有正在进行的分段上传任务 */
    public List<MultipartUpload> listMultipartUploads(String bucketName) {
        return s3Client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                .bucket(bucketName)
                .build()).uploads();
    }
    /** 列出特定对象和 uploadId 的所有已上传的部分 */
    public List<Part> listPartsForUpload(String bucketName, String key, String uploadId) {
        ListPartsRequest listPartsRequest = ListPartsRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build();

        ListPartsResponse listPartsResponse = s3Client.listParts(listPartsRequest);

        return listPartsResponse.parts();
    }

    /** 取消uploadId分片上传 */
    public void abortMultipartUpload(String bucketName, String objectName, String uploadId) {
        s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(objectName)
                .uploadId(uploadId)
                .build());
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
        try (InputStream inputStream = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectName)
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
            Path path = Paths.get(filePath);
            GetObjectRequest build = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build();
            s3Client.getObject(build,path);
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
            objectSize = getObject(bucketName, objectName).contentLength();
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
            try (InputStream inputStream = s3Client.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectName)
                            .range(rangeHeader)
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
}
