package com.yuanshuai.utils;


import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.crypto.digest.DigestUtil;
import com.aliyun.oss.model.PartETag;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.CreateBucketRequest;
import com.obs.services.model.ListBucketsRequest;
import com.obs.services.model.*;
import com.yuanshuai.domain.FileInfo;
import com.yuanshuai.domain.ListResult;
import io.minio.PutObjectArgs;
import io.minio.UploadObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Slf4j
public class OBSTool {

    private final ObsClient obsClient;


    public OBSTool(ObsClient obsClient) {
        this.obsClient = obsClient;
    }

    public void shutdown() {
        try {
            if (obsClient != null) {
                obsClient.close();
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
        BaseBucketRequest baseBucketRequest = new BaseBucketRequest();
        baseBucketRequest.setBucketName(bucketName);
        return obsClient.headBucket(baseBucketRequest);
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
            CreateBucketRequest createBucketRequest = new CreateBucketRequest();
            createBucketRequest.setBucketName(bucketName);
            obsClient.createBucket(createBucketRequest);
            return true;
        } catch (Exception e) {
            log.error("判断桶是否存在时发生错误: " + e.getMessage());
            return false;
        }
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
        HeaderResponse headerResponse = obsClient.deleteBucket(bucketName);
        return headerResponse.getStatusCode() == 204;
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
            if (tags == null || tags.isEmpty()) {
                log.info("标签为空，不设置标签");
                return false;
            }
            BucketTagInfo.TagSet tagSet = new BucketTagInfo.TagSet();
            tags.forEach(tagSet::addTag);

            BucketTagInfo bucketTagInfo = new BucketTagInfo();
            bucketTagInfo.setTagSet(tagSet);

            SetBucketTaggingRequest setBucketTaggingRequest = new SetBucketTaggingRequest();
            setBucketTaggingRequest.setBucketName(bucketName);
            setBucketTaggingRequest.setBucketTagInfo(bucketTagInfo);
            obsClient.setBucketTagging(setBucketTaggingRequest);

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
            return obsClient.getBucketTagging(bucketName)
                    .getTagSet()
                    .getTags()
                    .stream()
                    .collect(Collectors.toMap(BucketTagInfo.TagSet.Tag::getKey, BucketTagInfo.TagSet.Tag::getValue));
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
            obsClient.deleteBucketTagging(bucketName);
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
    public List<ObsBucket> listBuckets() {
        ListBucketsRequest listBucketsRequest = new ListBucketsRequest();
        List<ObsBucket> obsBuckets = obsClient.listBuckets(listBucketsRequest);
        return obsBuckets;
    }

    /********** 对象操作 **********/
    /**
     * 判断对象是否存在
     */
    public Boolean isObjectExists(String bucketName, String objectName) {
        GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest();
        getObjectMetadataRequest.setBucketName(bucketName);
        getObjectMetadataRequest.setObjectKey(objectName);
        return obsClient.doesObjectExist(getObjectMetadataRequest);
    }

    /**
     * 查询单文件
     *
     * @param bucketName
     * @param objectName
     * @return ObjectStat 文件信息
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public ObjectMetadata getObject(String bucketName, String objectName) {
        GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest();
        getObjectMetadataRequest.setObjectKey(objectName);
        getObjectMetadataRequest.setBucketName(bucketName);
        return obsClient.getObjectMetadata(getObjectMetadataRequest);
    }

    /**
     * 查询文件列表
     *
     * @param bucketName
     * @param prefix     前缀
     * @param maxKeys    最大数量
     * @return
     */
    public List<ObsObject> listObjects(String bucketName, String prefix, int maxKeys) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
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
    public List<ObsObject> listObjects(String bucketName, String prefix) {
        return listObjects(bucketName, prefix, 0);
    }
    public List<ObsObject> listObjects(String bucketName) {
        return listObjects(bucketName, null, 0);
    }

    /**
     * 查询多个文件
     *
     * @param bucketName
     * @param objectNameList
     * @return
     */
    public List<ObjectMetadata> listObjects(String bucketName, List<String> objectNameList) {
        return objectNameList.stream()
                .map(item -> getObject(bucketName, item))
                .collect(Collectors.toList());
    }

    /**
     * 删除单/多文件/某个目录下的所有文件，由于s3删除操作拿不到返回值，只能用Boolen表示是否成功
     *
     * @param bucketName
     * @param objectName
     * @return true/false
     */
    public DeleteObjectResult deleteObject(String bucketName, String objectName) {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest();
        deleteObjectRequest.setBucketName(bucketName);
        deleteObjectRequest.setObjectKey(objectName);
        return obsClient.deleteObject(deleteObjectRequest);
    }
    public List<ListResult> deleteObject(String bucketName, List<String> objectNames) {
        return objectNames.stream()
               .map(item -> new ListResult(item, deleteObject(bucketName, item).isDeleteMarker()))
               .collect(Collectors.toList());
    }
    public List<ListResult> deleteObjectDir(String bucketName, String dir) {
        List<ObsObject> obsObjects = listObjects(bucketName, dir);
        return obsObjects.stream()
                .map(item -> new ListResult(item.getObjectKey(), deleteObject(bucketName, item.getObjectKey()).isDeleteMarker()))
                .collect(Collectors.toList());
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
    public CopyObjectResult copyObject(String sourceBucketName, String sourceObjectName, String targetBucketName, String targetObjectName) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest();
        copyObjectRequest.setSourceBucketName(sourceBucketName);
        copyObjectRequest.setSourceObjectKey(sourceObjectName);
        copyObjectRequest.setBucketName(targetBucketName);
        copyObjectRequest.setObjectKey(targetObjectName);
        return obsClient.copyObject(copyObjectRequest);

    }
    public List<CopyObjectResult> copyObjectList(String sourceBucketName, List<String> sourceObjectName, String targetBucketName) {
        List<CopyObjectResult> collect = sourceObjectName.stream().map(
                sourceObject -> copyObject(sourceBucketName, sourceObject, targetBucketName, sourceObject)
        ).collect(Collectors.toList());
        return collect;
    }
    public List<CopyObjectResult> copyBucket(String sourceBucketName, String targetBucketName) {
        List<ObsObject> obsObjects = listObjects(sourceBucketName, null, 0);
        return obsObjects.stream().map(obsObject -> {
            String sourceObjectName = obsObject.getObjectKey();
            return copyObject(sourceBucketName, sourceObjectName, targetBucketName, sourceObjectName);
        }).collect(Collectors.toList());
    }
    /**
     * 复制某个目录到其他目录，目录下所有文件都复制到目标目录下
     */
    public List<CopyObjectResult> copyObjectDir(String sourceBucketName, String sourceDir, String targetBucketName, String targetDir) {
        List<ObsObject> obsObjects = listObjects(sourceBucketName, sourceDir, 0);
        return obsObjects.stream().map(obsObject -> {
            String sourceObjectName = obsObject.getObjectKey();
            return copyObject(sourceBucketName, sourceObjectName, targetBucketName, targetDir + sourceObjectName.substring(sourceDir.length()));
        }).collect(Collectors.toList());
    }

    /********** 上传操作 **********/
    /**
     * 简单上传，MultipartFile类型上传单文件
     *
     * @param bucketName 桶名
     * @param objectName 对象名
     * @param file       为MultipartFile类型
     * @param userMetadata 用户自定义元数据
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file, Map<String, Object> userMetadata) {
        PutObjectRequest putObjectRequest = new PutObjectRequest();
        try (InputStream fis = file.getInputStream()) {
            String md5 = DigestUtil.md5Hex(fis);
            userMetadata.put("File-Md5", md5);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setMetadata(userMetadata);
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
    public Boolean uploadFile(String bucketName, String objectName, MultipartFile file){
        return uploadFile(bucketName, objectName, file, new HashMap<>());
    }

    /**
     * 简单上传，InputStream类型上传单文件
     *
     * @param bucketName  桶名
     * @param objectName  对象名
     * @param inputStream 输入流
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream, Map<String, Object> userMetadata) {
        try {
            String md5 = DigestUtil.md5Hex(inputStream);
            userMetadata.put("File-Md5", md5);
            PutObjectRequest putObjectRequest = new PutObjectRequest();
            putObjectRequest.setInput(inputStream);
            putObjectRequest.setBucketName(bucketName);
            putObjectRequest.setObjectKey(objectName);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setMetadata(userMetadata);
            putObjectRequest.setMetadata(objectMetadata);

            obsClient.putObject(putObjectRequest);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String bucketName, String objectName, InputStream inputStream) {
        return uploadFile(bucketName, objectName, inputStream, new HashMap<>());
    }

    /**
     * 简单上传，本地文件类型上传单文件
     *
     * @param bucketName 桶名
     * @param objectName 对象名
     * @param filePath   文件路径
     * @return true/false
     */
    public Boolean uploadFile(String bucketName, String objectName, String filePath, Map<String, Object> userMetadata) {
        try {
            File file = new File(filePath);
            PutObjectRequest putObjectRequest = new PutObjectRequest();
            putObjectRequest.setFile(file);
            putObjectRequest.setBucketName(bucketName);
            putObjectRequest.setObjectKey(objectName);

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setMetadata(userMetadata);
            putObjectRequest.setMetadata(objectMetadata);

            obsClient.putObject(putObjectRequest);
            log.info("上传文件" + objectName + "成功");
            return true;
        } catch (Exception e) {
            log.error("上传文件时发生错误: " + e.getMessage());
        }
        return false;
    }
    public Boolean uploadFile(String objectName, String bucketName, String filePath) {
        return uploadFile(objectName, bucketName, filePath, new HashMap<>());
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
    public AppendObjectResult appendUpload(String bucketName, String targetObjectName, MultipartFile file) {
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest();
        try (InputStream inputStream = file.getInputStream()){

            appendObjectRequest.setBucketName(bucketName);
            appendObjectRequest.setObjectKey(targetObjectName);

            ObjectMetadata objectMetadata = getObject(bucketName, targetObjectName);
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
    public CompleteMultipartUploadResult uploadFileWithCheckpoint(String filePath, String bucketName, String objectName, Long partSize, Integer taskNum, Boolean enableCheckpoint) {
        UploadFileRequest uploadFileRequest = new UploadFileRequest(bucketName, objectName);
        uploadFileRequest.setUploadFile(filePath);
        uploadFileRequest.setEnableCheckpoint(enableCheckpoint);
        uploadFileRequest.setTaskNum(taskNum);
        uploadFileRequest.setPartSize(partSize);
        return obsClient.uploadFile(uploadFileRequest);
    }
    public CompleteMultipartUploadResult uploadFileWithCheckpoint(String filePath, String bucketName, String objectName) {
        // 9MB
        return uploadFileWithCheckpoint(filePath, bucketName, objectName, 9 * 1024 * 1024L, 1, false);
    }




    /** 分片上传 */
    /** 初始化分片任务 */
    public String getUploadId(String bucketName, String objectName) {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest();
        initiateMultipartUploadRequest.setBucketName(bucketName);
        initiateMultipartUploadRequest.setObjectKey(objectName);
        return obsClient.initiateMultipartUpload(initiateMultipartUploadRequest).getUploadId();
    }

    /** 上传uploadId分片 objectName任务名字 */
    public Boolean multipartUpload(FileInfo obsFileInfo, List<MultipartFile> files) {
        List<FileInfo.PartInfo> parts = obsFileInfo.getParts();
        String bucketName = obsFileInfo.getBucketName();
        String objectName = obsFileInfo.getObjectName();
        String uploadId = obsFileInfo.getUploadId();
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        for (int i = 0; i < parts.size(); i++) {
            Integer partNumber = Convert.toInt(parts.get(i).getCurrentNum());
            MultipartFile file = files.get(i);
            try (InputStream inputStream = file.getInputStream()){
                uploadPartRequest.setBucketName(bucketName);
                uploadPartRequest.setObjectKey(objectName);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setPartNumber(partNumber);
                uploadPartRequest.setInput(inputStream);
                obsClient.uploadPart(uploadPartRequest);
            } catch (IOException e) {
                log.error("上传分片" + partNumber + "时发生错误: " + e.getMessage(), e);
                return false;
            }
        }
        return true;
    }

    /** uploadId分片合并 */
    public Boolean completeMultipartUpload(FileInfo obsFileInfo) {
        try {
            String bucketName = obsFileInfo.getBucketName();
            String objectName = obsFileInfo.getObjectName();
            String uploadId = obsFileInfo.getUploadId();
            List<FileInfo.PartInfo> parts = obsFileInfo.getParts();
            List<PartEtag> partEtags = parts.stream()
                    .map(part -> new PartEtag(part.getPartMd5(),Convert.toInt(part.getCurrentNum())))
                    .collect(Collectors.toList());
            CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
            completeMultipartUploadRequest.setBucketName(bucketName);
            completeMultipartUploadRequest.setObjectKey(objectName);
            completeMultipartUploadRequest.setUploadId(uploadId);
            completeMultipartUploadRequest.setPartEtag(partEtags);
            obsClient.completeMultipartUpload(completeMultipartUploadRequest);
            return true;
        } catch (ObsException e) {
            log.error("合并分片上传时发生错误: " + e.getMessage(), e);
            return false;
        }
    }

    /** 取消uploadId分片上传 */
    public HeaderResponse abortMultipartUpload(String bucketName, String objectName, String uploadId) {
        AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest();
        abortMultipartUploadRequest.setBucketName(bucketName);
        abortMultipartUploadRequest.setObjectKey(objectName);
        abortMultipartUploadRequest.setUploadId(uploadId);
        return obsClient.abortMultipartUpload(abortMultipartUploadRequest);
    }


    /** 查询uploadId分片信息列表 */
    public ListPartsResult listParts(String bucketName, String objectName, String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest();
        listPartsRequest.setBucketName(bucketName);
        listPartsRequest.setObjectKey(objectName);
        listPartsRequest.setUploadId(uploadId);
        return obsClient.listParts(listPartsRequest);
    }


    /** 查询桶中所有分片上传任务 */
    public MultipartUploadListing listMultipartUploads(String bucketName) {
        ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest();
        listMultipartUploadsRequest.setBucketName(bucketName);
        return obsClient.listMultipartUploads(listMultipartUploadsRequest);

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
     * 获取对象部分流
     */
    public InputStream getFileRangeStream(String bucketName, String objectName, Long start, Long end) {
        GetObjectRequest getObjectRequest = new GetObjectRequest();
        getObjectRequest.setRangeStart(start);
        getObjectRequest.setRangeStart(end);
        getObjectRequest.setBucketName(bucketName);
        getObjectRequest.setObjectKey(objectName);
        return obsClient.getObject(getObjectRequest).getObjectContent();
    }

    /**
     * 下载文件到本地指定位置
     *
     * @param objectName 文件名
     * @param bucketName 桶名
     * @param filePath   本地路径，复杂路径需要用URLUtil.encode(filePath, CharsetUtil.CHARSET_UTF_8)处理才能当参数传递
     * @param partSize   分片大小，单位字节
     * @param taskNum    并发任务数
     * @return true/false
     * 解释：不提供bucketName，默认使用当前yml配置的桶
     */
    public DownloadFileResult downloadFileWithCheckpoint(String filePath, String bucketName, String objectName, Long partSize, Integer taskNum,Boolean enableCheckpoint) {
        DownloadFileRequest downloadFileRequest = new DownloadFileRequest(bucketName,objectName);
        downloadFileRequest.setDownloadFile(filePath);
        downloadFileRequest.setTaskNum(taskNum);
        downloadFileRequest.setEnableCheckpoint(enableCheckpoint);
        downloadFileRequest.setPartSize(partSize);
        return downloadFileWithCheckpoint(downloadFileRequest);
    }
    public DownloadFileResult downloadFileWithCheckpoint(String filePath, String bucketName, String objectName, Long partSize, Integer taskNum) {
        return downloadFileWithCheckpoint(filePath, bucketName, objectName, partSize, taskNum,false);
    }
    private DownloadFileResult downloadFileWithCheckpoint(DownloadFileRequest downloadFileRequest) {
        return obsClient.downloadFile(downloadFileRequest);
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
        ObjectMetadata objectMetadata = getObject(bucketName, objectName);
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
            try (InputStream inputStream = getFileRangeStream(bucketName, objectName, start, end);
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