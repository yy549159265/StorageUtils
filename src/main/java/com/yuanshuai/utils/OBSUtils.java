package com.yuanshuai.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.*;
import com.yuanshuai.config.OscConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class OBSUtils {
    public S3Client s3Client;
    public ObsClient obsClient;

    private final String GbucketName;
    private final String Gregion;

    public OBSUtils(OscConfig config, ObsConfiguration obsConfiguration) {
        obsConfiguration.setEndPoint(config.getEndpoint());
        obsClient = new ObsClient(config.getAccessKey(), config.getSecretKey(), obsConfiguration);
        GbucketName = config.getBucketName();
        Gregion = config.getRegion();
    }
    /** 华为云兼容s3 */
    public OBSUtils(OscConfig config, S3ClientBuilder builder) {
        Region region = Region.of(config.getRegion());
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                config.getAccessKey(), // AK，即Access Key ID
                config.getSecretKey() // SK，即Secret Access Key
        );
        URI endpointuri = URI.create(config.getEndpoint());
        s3Client = builder.region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .endpointOverride(endpointuri)
                .build();
        GbucketName = config.getBucketName();
        Gregion = config.getRegion();
    }

    public void shutdown() {
        try {
            obsClient.close();
            s3Client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    /********************* 桶操作 **********/
    /**
     * 判断桶是否存在
     *
     * @param bucketName 桶名
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public Boolean isBucketExists(String bucketName) {
        return obsClient.headBucket(bucketName);
    }

    public Boolean isBucketExists() {
        return isBucketExists(GbucketName);
    }

    /**
     * 创建桶
     *
     * @param bucketName 桶名
     * @return ObsBucket
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public ObsBucket createBucket(String bucketName, String region) {
        ObsBucket bucket = obsClient.createBucket(bucketName, region);
        return bucket;
    }

    public ObsBucket createBucket() {
        return createBucket(GbucketName, Gregion);
    }

    /**
     * 删除桶
     *
     * @param bucketName 桶名
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public HeaderResponse deleteBucket(String bucketName) {
        HeaderResponse headerResponse = obsClient.deleteBucket(bucketName);
        return headerResponse;
    }

    public HeaderResponse deleteBucket() {
        return deleteBucket(GbucketName);

    }

    /**
     * 列出所有桶
     *
     * @return 桶名列表
     */
    public List<ObsBucket> listBuckets(ListBucketsRequest listBucketsRequest) {
        List<ObsBucket> obsBuckets = obsClient.listBuckets(listBucketsRequest);
        return obsBuckets;
    }

    /**
     * 获取桶信息
     *
     * @return 桶信息
     */
    public String getBucketLocation(String bucketName) {
        return obsClient.getBucketLocation(bucketName);
    }

    public BucketStorageInfo getBucketStorageInfo(String bucketName) {
        BucketStorageInfo bucketStorageInfo = obsClient.getBucketStorageInfo(bucketName);
        return bucketStorageInfo;
    }

    public BucketQuota getBucketQuota(String bucketName) {
        BucketQuota bucketQuota = obsClient.getBucketQuota(bucketName);
        return bucketQuota;
    }

    public BucketStoragePolicyConfiguration getBucketStoragePolicy(String bucketName) {
        BucketStoragePolicyConfiguration bucketStoragePolicy = obsClient.getBucketStoragePolicy(bucketName);
        return bucketStoragePolicy;
    }

    /**
     * 设置桶信息
     */
    public HeaderResponse setBucketQuota(String bucketName, BucketQuota bucketQuota) {
        HeaderResponse headerResponse = obsClient.setBucketQuota(bucketName, bucketQuota);
        return headerResponse;
    }

    public HeaderResponse setBucketStoragePolicy(String bucketName, BucketStoragePolicyConfiguration bucketStoragePolicy) {
        HeaderResponse headerResponse = obsClient.setBucketStoragePolicy(bucketName, bucketStoragePolicy);
        return headerResponse;
    }

    /**
     * 设置桶策略，获取桶策略，删除桶策略
     */

    public HeaderResponse setBucketPolicy(String bucketName, String policyJson) {
        HeaderResponse headerResponse = obsClient.setBucketPolicy(bucketName, policyJson);
        return headerResponse;
    }

    public BucketPolicyResponse getBucketPolicy(String bucketName) {
        return obsClient.getBucketPolicyV2(bucketName);
    }

    public HeaderResponse deleteBucketPolicy(String bucketName) {
        return obsClient.deleteBucketPolicy(bucketName);
    }


    /********************* 文件操作 **********/
    /**
     * 查询单文件
     *
     * @param bucketName
     * @param objectName
     * @return ObjectStat 文件信息
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public ObjectMetadata getObjectMetadata(String bucketName, String objectName, GetObjectMetadataRequest getObjectMetadataRequest) {
        getObjectMetadataRequest.setObjectKey(objectName);
        getObjectMetadataRequest.setBucketName(bucketName);
        ObjectMetadata objectMetadata = obsClient.getObjectMetadata(getObjectMetadataRequest);
        return objectMetadata;
    }

    public ObjectMetadata getObjectMetadata(String objectName) {
        return getObjectMetadata(GbucketName, objectName, new GetObjectMetadataRequest());
    }

    /**
     * 判断文件是否存在
     */
    public Boolean isObjectExists(String objectName) {
        return isObjectExists(GbucketName, objectName, new GetObjectMetadataRequest());
    }

    public Boolean isObjectExists(String bucketName, String objectName, GetObjectMetadataRequest getObjectMetadataRequest) {
        getObjectMetadataRequest.setBucketName(bucketName);

        getObjectMetadataRequest.setObjectKey(objectName);
        return obsClient.doesObjectExist(getObjectMetadataRequest);
    }

    /**
     * 查询文件列表
     *
     * @param bucketName
     * @param listObjectsRequest
     * @return ObjectStat 文件信息
     * 你可以设置额外的信息，如bucketName，prefix，maxKeys等。更多参数请写入到ListObjectsRequest中。
     */
    public List<ObsObject> listObjects(String bucketName, String prefix, int maxKeys, ListObjectsRequest listObjectsRequest) {
        listObjectsRequest.setBucketName(bucketName);
        if (prefix != null) {
            listObjectsRequest.setPrefix(prefix);
        }
        if (maxKeys > 1) {
            listObjectsRequest.setMaxKeys(maxKeys);
        }
        ObjectListing objectListing = obsClient.listObjects(listObjectsRequest);
        List<ObsObject> objects = objectListing.getObjects();
        return objects;
    }

    public List<ObsObject> listObjects() {
        return listObjects(GbucketName, null, 0, new ListObjectsRequest());
    }


    /**
     * 删除单/多文件/某个目录下的所有文件
     *
     * @param bucketName
     * @param objectName
     * @return true/false
     */
    public DeleteObjectResult deleteObject(String bucketName, String objectName, DeleteObjectRequest deleteObjectRequest) {
        deleteObjectRequest.setBucketName(bucketName);
        deleteObjectRequest.setObjectKey(objectName);
        DeleteObjectResult deleteObjectResult = obsClient.deleteObject(deleteObjectRequest);
        return deleteObjectResult;
    }

    public DeleteObjectResult deleteObject(String objectName) {
        return deleteObject(GbucketName, objectName, new DeleteObjectRequest());
    }

    public List<DeleteObjectsResult.DeleteObjectResult> deleteObjectList(String bucketName, String objectName, DeleteObjectsRequest deleteObjectsRequest) {
        deleteObjectsRequest.setBucketName(bucketName);
        DeleteObjectsResult deleteObjectsResult = obsClient.deleteObjects(deleteObjectsRequest);
        List<DeleteObjectsResult.DeleteObjectResult> deletedObjectResults = deleteObjectsResult.getDeletedObjectResults();
        deleteObjectsResult.getErrorResults().stream().forEach(error -> {
            log.error("delete object error, code:{}, message:{}, key:{}", error.getErrorCode(), error.getMessage(), error.getObjectKey());
        });
        return deletedObjectResults;
    }

    public List<DeleteObjectsResult.DeleteObjectResult> deleteObjectList(String objectName) {
        return deleteObjectList(GbucketName, objectName, new DeleteObjectsRequest());
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
    public CopyObjectResult copyObject(String sourceBucketName, String sourceObjectName, String targetBucketName, String targetObjectName, CopyObjectRequest copyObjectRequest) {
        copyObjectRequest.setSourceBucketName(sourceBucketName);
        copyObjectRequest.setSourceObjectKey(sourceObjectName);
        copyObjectRequest.setBucketName(targetBucketName);
        copyObjectRequest.setObjectKey(targetObjectName);
        CopyObjectResult copyObjectResult = obsClient.copyObject(copyObjectRequest);
        return copyObjectResult;

    }

    public CopyObjectResult copyObject(String sourceBucketName, String sourceObjectName, String targetBucketName, String targetObjectName) {
        return copyObject(sourceBucketName, sourceObjectName, targetBucketName, targetObjectName, new CopyObjectRequest());
    }

    public List<CopyObjectResult> copyObjectList(String sourceBucketName, List<String> sourceObjectName, String targetBucketName) {
        List<CopyObjectResult> collect = sourceObjectName.stream().map(
                sourceObject -> copyObject(sourceBucketName, sourceObject, targetBucketName, sourceObject)
        ).collect(Collectors.toList());
        return collect;
    }

    public List<CopyObjectResult> copyBucket(String sourceBucketName, String targetBucketName) {
        List<ObsObject> obsObjects = listObjects(sourceBucketName, null, 0, new ListObjectsRequest());
        return obsObjects.stream().map(obsObject -> {
            String sourceObjectName = obsObject.getObjectKey();
            return copyObject(sourceBucketName, sourceObjectName, targetBucketName, sourceObjectName);
        }).collect(Collectors.toList());
    }

    /**
     * 复制某个目录到其他目录，目录下所有文件都复制到目标目录下
     */
    public List<CopyObjectResult> copyObjectDir(String sourceBucketName, String sourceDir, String targetBucketName, String targetDir) {
        List<ObsObject> obsObjects = listObjects(sourceBucketName, sourceDir, 0, new ListObjectsRequest());
        return obsObjects.stream().map(obsObject -> {
            String sourceObjectName = obsObject.getObjectKey();
            return copyObject(sourceBucketName, sourceObjectName, targetBucketName, targetDir + sourceObjectName.substring(sourceDir.length()));
        }).collect(Collectors.toList());
    }


    /********************* 下载操作 **********/
    /**
     * 简单下载，直接下载单文件
     *
     * @param objectName 文件名
     * @param bucketName 桶名
     * @param response   响应
     * @return true/false
     */
    public void simpleDownload(String bucketName, String objectName, HttpServletResponse response) {
        try (InputStream inputStream = obsClient.getObject(bucketName, objectName).getObjectContent();
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
     * 获取对象流
     */
    public InputStream getObjectInputStream(String objectName) {
        return getObjectInputStream(GbucketName, objectName, new GetObjectRequest());
    }

    public InputStream getObjectInputStream(String bucketName, String objectName, GetObjectRequest getObjectRequest) {
        getObjectRequest.setBucketName(bucketName);
        getObjectRequest.setObjectKey(objectName);
        return obsClient.getObject(getObjectRequest).getObjectContent();
    }

    /**
     * 获取对象部分流
     */
    public InputStream getRangeInputStream(String bucketName, String objectName, Long start, Long end, GetObjectRequest getObjectRequest) {
        getObjectRequest.setRangeStart(start);
        getObjectRequest.setRangeStart(end);
        getObjectRequest.setBucketName(bucketName);
        getObjectRequest.setObjectKey(objectName);
        return obsClient.getObject(getObjectRequest).getObjectContent();
    }

    public InputStream getRangeInputStream(String objectName, Long start, Long end) {
        return getRangeInputStream(GbucketName, objectName, start, end, new GetObjectRequest());
    }

    /**
     * 获取对象信息
     */
    public ObsObject getObject(String bucketName, String objectName, GetObjectRequest getObjectRequest) {
        getObjectRequest.setBucketName(bucketName);
        getObjectRequest.setObjectKey(objectName);
        ObsObject object = obsClient.getObject(getObjectRequest);
        return object;
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
        ObjectMetadata objectMetadata = getObjectMetadata(bucketName, objectName, new GetObjectMetadataRequest());
        if (objectMetadata != null) {
            objectSize = objectMetadata.getContentLength();
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
            GetObjectRequest getObjectRequest = new GetObjectRequest();
            getObjectRequest.setRangeStart(start);
            getObjectRequest.setRangeEnd(end);
            try (InputStream inputStream = getObjectInputStream(bucketName, objectName, getObjectRequest);
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


    /**
     * 华为云方式断点下载
     *
     * @param downloadFileRequest 自定义断点续传
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public DownloadFileResult filePathDownload(DownloadFileRequest downloadFileRequest) {
        DownloadFileResult downloadFileResult = obsClient.downloadFile(downloadFileRequest);
        return downloadFileResult;
    }

    /**
     * 华为云方式断点下载
     */
    public DownloadFileResult filePathDownload(String filePath, String objectName) {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(GbucketName, objectName);
        downloadFileRequest.setDownloadFile(filePath);
        downloadFileRequest.setTaskNum(5);
        downloadFileRequest.setEnableCheckpoint(true);
        return filePathDownload(downloadFileRequest);
    }
    public DownloadFileResult filePathDownloadNoCheckpoint(String filePath, String objectName) {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(GbucketName, objectName);
        downloadFileRequest.setDownloadFile(filePath);
        downloadFileRequest.setTaskNum(5);
        downloadFileRequest.setEnableCheckpoint(false);
        return filePathDownload(downloadFileRequest);
    }
    public DownloadFileResult filePathDownload(String filePath, String objectName, Long partSize, Integer taskNum) {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(GbucketName, objectName);
        downloadFileRequest.setDownloadFile(filePath);
        downloadFileRequest.setTaskNum(taskNum);
        downloadFileRequest.setEnableCheckpoint(true);
        downloadFileRequest.setPartSize(partSize);
        return filePathDownload(downloadFileRequest);
    }


    /****************************************/

    /********************* 上传操作 **********/
    /*** 建议上传使用application/octet-stream，避免兼容问题 ***/
    /**
     * 简单上传，直接上传单文件
     *
     * @param bucketName
     * @param objectName
     * @param file       注意配置文件multipart限制
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     * 如果上传文件大于5mb，使用application/octet-stream
     */
    public Boolean simpleUpload(String bucketName, String objectName, MultipartFile file,PutObjectRequest putObjectRequest) {
        Map<String, Object> md5Map = new HashMap<>();
        try (InputStream fis = file.getInputStream()) {
            String md5 = DigestUtil.md5Hex(fis);
            md5Map.put("fileMd5", md5);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setMetadata(md5Map);

            putObjectRequest.setInput(fis);
            putObjectRequest.setBucketName(bucketName);
            putObjectRequest.setObjectKey(objectName);
            putObjectRequest.setMetadata(objectMetadata);

            obsClient.putObject(putObjectRequest);
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }

    public Boolean simpleUpload(String objectName, MultipartFile file) {
        return simpleUpload(GbucketName, objectName, file,new PutObjectRequest());
    }

    /**
     * 追加新内容上传，请确认桶类型，并行文件系统不支持追加上传。请确认对象类型，归档存储和深度归档存储对象不支持追加上传。
     *
     * @param bucketName
     * @param targetObjectName
     * @param file
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public AppendObjectResult appendUpload(String bucketName, String targetObjectName, MultipartFile file,AppendObjectRequest appendObjectRequest) {
        try (InputStream inputStream = file.getInputStream()){

            appendObjectRequest.setBucketName(bucketName);
            appendObjectRequest.setObjectKey(targetObjectName);

            ObjectMetadata objectMetadata = getObjectMetadata(bucketName, targetObjectName, new GetObjectMetadataRequest());
            appendObjectRequest.setPosition(objectMetadata.getNextPosition());
            appendObjectRequest.setInput(inputStream);
            AppendObjectResult appendObjectResult = obsClient.appendObject(appendObjectRequest);
            return appendObjectResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * 上传本地文件到桶中，可指定是否断点续传
     *
     * @return true/false
     */
    public CompleteMultipartUploadResult filePathUpload(UploadFileRequest uploadFileRequest) {
        CompleteMultipartUploadResult completeMultipartUploadResult = obsClient.uploadFile(uploadFileRequest);
        return completeMultipartUploadResult;
    }
    public CompleteMultipartUploadResult filePathUploadNoCheckpoint(String filePath, String objectName) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(GbucketName, objectName);
        uploadFileRequest.setUploadFile(filePath);
        uploadFileRequest.setEnableCheckpoint(false);
        uploadFileRequest.setTaskNum(5);
        CompleteMultipartUploadResult completeMultipartUploadResult = obsClient.uploadFile(uploadFileRequest);
        return completeMultipartUploadResult;
    }
    public CompleteMultipartUploadResult filePathUpload(String filePath, String objectName) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(GbucketName, objectName);
        uploadFileRequest.setUploadFile(filePath);
        uploadFileRequest.setEnableCheckpoint(true);
        uploadFileRequest.setTaskNum(5);
        CompleteMultipartUploadResult completeMultipartUploadResult = obsClient.uploadFile(uploadFileRequest);
        return completeMultipartUploadResult;
    }
    public CompleteMultipartUploadResult filePathUpload(String filePath, String objectName, Long partSize, Integer taskNum) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(GbucketName, objectName);
        uploadFileRequest.setUploadFile(filePath);
        uploadFileRequest.setEnableCheckpoint(true);
        uploadFileRequest.setTaskNum(taskNum);
        uploadFileRequest.setPartSize(partSize);
        CompleteMultipartUploadResult completeMultipartUploadResult = obsClient.uploadFile(uploadFileRequest);
        return completeMultipartUploadResult;
    }

    /** 分片上传 */
    /** 初始化分片任务 */
    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String objectName, InitiateMultipartUploadRequest initiateMultipartUploadRequest) {
        initiateMultipartUploadRequest.setBucketName(bucketName);
        initiateMultipartUploadRequest.setObjectKey(objectName);

        InitiateMultipartUploadResult initiateMultipartUploadResult = obsClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult;
    }
    public InitiateMultipartUploadResult initiateMultipartUpload(String objectName) {
        return initiateMultipartUpload(GbucketName, objectName, new InitiateMultipartUploadRequest());
    }

    /** 上传uploadId分片 objectName任务名字 */
    public UploadPartResult uploadPart(String bucketName, String objectName, String uploadId, int partNumber, MultipartFile file, UploadPartRequest uploadPartRequest) {

        try (InputStream inputStream = file.getInputStream()){

            uploadPartRequest.setBucketName(bucketName);
            uploadPartRequest.setObjectKey(objectName);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setPartNumber(partNumber);
            uploadPartRequest.setInput(inputStream);
            UploadPartResult uploadPartResult = obsClient.uploadPart(uploadPartRequest);
            return uploadPartResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public UploadPartResult uploadPart(String objectName, String uploadId, int partNumber, MultipartFile file) {
        return uploadPart(GbucketName, objectName, uploadId, partNumber, file, new UploadPartRequest());
    }

    /** uploadId分片合并 */
    public CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String objectName, String uploadId, List<PartEtag> partEtags, CompleteMultipartUploadRequest completeMultipartUploadRequest) {
        completeMultipartUploadRequest.setBucketName(bucketName);
        completeMultipartUploadRequest.setObjectKey(objectName);
        completeMultipartUploadRequest.setUploadId(uploadId);
        completeMultipartUploadRequest.setPartEtag(partEtags);
        CompleteMultipartUploadResult completeMultipartUploadResult = obsClient.completeMultipartUpload(completeMultipartUploadRequest);
        return completeMultipartUploadResult;
    }
    public CompleteMultipartUploadResult completeMultipartUpload(String objectName, String uploadId, List<PartEtag> partEtags) {
        return completeMultipartUpload(GbucketName, objectName, uploadId, partEtags, new CompleteMultipartUploadRequest());
    }

    /** 取消uploadId分片上传 */
    public HeaderResponse abortMultipartUpload(String bucketName, String objectName, String uploadId, AbortMultipartUploadRequest abortMultipartUploadRequest) {
        abortMultipartUploadRequest.setBucketName(bucketName);
        abortMultipartUploadRequest.setObjectKey(objectName);
        abortMultipartUploadRequest.setUploadId(uploadId);
        HeaderResponse headerResponse = obsClient.abortMultipartUpload(abortMultipartUploadRequest);
        return headerResponse;
    }
    public HeaderResponse abortMultipartUpload(String objectName, String uploadId) {
        return abortMultipartUpload(GbucketName, objectName, uploadId, new AbortMultipartUploadRequest());
    }

    /** 查询uploadId分片信息列表 */
    public ListPartsResult listParts(String bucketName, String objectName, String uploadId, ListPartsRequest listPartsRequest) {
        listPartsRequest.setBucketName(bucketName);
        listPartsRequest.setObjectKey(objectName);
        listPartsRequest.setUploadId(uploadId);
        ListPartsResult listPartsResult = obsClient.listParts(listPartsRequest);
        return listPartsResult;
    }
    public ListPartsResult listParts(String objectName, String uploadId) {
        return listParts(GbucketName, objectName, uploadId, new ListPartsRequest());
    }

    /** 查询桶中所有分片上传任务 */
    public MultipartUploadListing listMultipartUploads(String bucketName, ListMultipartUploadsRequest listMultipartUploadsRequest) {
        listMultipartUploadsRequest.setBucketName(bucketName);
        MultipartUploadListing listMultipartUploadsResult = obsClient.listMultipartUploads(listMultipartUploadsRequest);

        return listMultipartUploadsResult;

    }
    public MultipartUploadListing listMultipartUploads() {
        return listMultipartUploads(GbucketName, new ListMultipartUploadsRequest());
    }
    /****************************************/


}
