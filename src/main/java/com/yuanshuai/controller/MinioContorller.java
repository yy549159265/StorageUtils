package com.yuanshuai.controller;


import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.URLUtil;
import com.yuanshuai.api.CommonResult;
import com.yuanshuai.config.StorageConfig;
import com.yuanshuai.constants.StorageType;
import com.yuanshuai.domain.FileInfo;
import com.yuanshuai.domain.ListResult;
import com.yuanshuai.factory.StorageClientFactory;
import com.yuanshuai.factory.StorageUtilsFactory;
import com.yuanshuai.utils.MinioTool;
import io.minio.MinioClient;
import io.minio.StatObjectResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/minio")
public class MinioContorller {

    private final StorageConfig storageConfig;
    private final MinioTool utils;

    public MinioContorller(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        MinioClient client = StorageClientFactory.createClient(StorageType.MINIO, storageConfig);
        this.utils = StorageUtilsFactory.createUtils(StorageType.MINIO, client);
    }

    // 查询桶是否存在
    @GetMapping("/bucketExists")
    public CommonResult<Boolean> bucketExists(@RequestParam(value = "bucketName") String bucketName) {
        Boolean bucketExists = utils.isBucketExists(bucketName);
        return bucketExists ? CommonResult.success(bucketExists) : CommonResult.failed(bucketExists);

    }

    // 查询桶列表是否存在
    @GetMapping("/bucketExistsList")
    public CommonResult<List<ListResult>> bucketExistsList(@RequestParam(value = "bucketNames") List<String> bucketNames) {
        List<ListResult> bucketExists = utils.isBucketExists(bucketNames);
        return CommonResult.success(bucketExists);
    }

    // 创建桶
    @PostMapping("/createBucket")
    public CommonResult<Boolean> createBucket(@RequestParam(value = "bucketNames") String bucketName) {
        Boolean bucketExists = utils.createBucket(bucketName);
        return bucketExists ? CommonResult.success(bucketExists) : CommonResult.failed(bucketExists);
    }

    // 创建桶列表
    @PostMapping("/createBucketList")
    public CommonResult<List<ListResult>> createBucketList(@RequestBody List<String> bucketNames) {
        List<ListResult> bucketExists = utils.createBucket(bucketNames);
        return CommonResult.success(bucketExists);
    }

    // 删除桶
    @DeleteMapping("/deleteBucket")
    public CommonResult<Boolean> deleteBucket(@RequestParam(value = "bucketNames") String bucketName) {
        Boolean bucketExists = utils.deleteBucket(bucketName);
        return bucketExists ? CommonResult.success(bucketExists) : CommonResult.failed(bucketExists);
    }

    // 删除桶列表
    @DeleteMapping("/deleteBucketList")
    public CommonResult<List<ListResult>> deleteBucketList(@RequestBody List<String> bucketNames) {
        List<ListResult> bucketExists = utils.deleteBucket(bucketNames);
        return CommonResult.success(bucketExists);
    }

    // 列出桶
    @GetMapping("/listBuckets")
    public CommonResult<List<String>> listBuckets() {
        List<String> bucketNames = utils.listBuckets();
        return CommonResult.success(bucketNames);
    }

    // 设置桶标签
    @PostMapping("/setBucketTags")
    public CommonResult<Boolean> setBucketTags(@RequestParam(value = "bucketName") String bucketName, @RequestBody Map<String, String> tags) {
        Boolean bucketExists = utils.setBucketTags(bucketName, tags);
        return bucketExists ? CommonResult.success(bucketExists) : CommonResult.failed(bucketExists);
    }

    // 获取桶标签
    @GetMapping("/getBucketTags")
    public CommonResult<Map<String, String>> getBucketTags(@RequestParam(value = "bucketName") String bucketName) {
        Map<String, String> bucketTags = utils.getBucketTags(bucketName);
        return CommonResult.success(bucketTags);
    }


    @GetMapping("/objectExists")
    public CommonResult<Boolean> getPresignedUrl(@RequestParam(value = "bucketName") String bucketName,
                                                 @RequestParam(value = "objectName") String objectName) {
        Boolean objectExists = utils.isObjectExists(bucketName, objectName);
        return objectExists ? CommonResult.success(objectExists) : CommonResult.failed(objectExists);
    }

    // 获取文件
    @GetMapping("/getObject")
    public CommonResult<String> getObject(@RequestParam(value = "bucketName") String bucketName,
                                                     @RequestParam(value = "objectName") String objectName) {
        StatObjectResponse object = utils.getObject(bucketName, objectName);
        return CommonResult.success(object.toString());
    }

    // 复制文件
    @PostMapping("/copyObject")
    public CommonResult<Boolean> copyObject(@RequestParam(value = "bucketName") String bucketName,
                                            @RequestParam(value = "objectName") String objectName,
                                            @RequestParam(value = "destBucketName") String destBucketName,
                                            @RequestParam(value = "destObjectName") String destObjectName) {
        Boolean objectExists = utils.copyObject(bucketName, objectName, destBucketName, destObjectName);
        return objectExists ? CommonResult.success(objectExists) : CommonResult.failed(objectExists);
    }

    // 复制多文件
    @PostMapping("/copyObjects")
    public CommonResult<List<ListResult>> copyObjects(@RequestParam(value = "bucketName") String bucketName,
                                                      @RequestParam(value = "destBucketName") String destBucketName,
                                                      @RequestBody List<String> objectNames) {
        List<ListResult> objectExists = utils.copyObjectList(bucketName, objectNames, destBucketName);
        return CommonResult.success(objectExists);
    }

    // 复制目录
    @PostMapping("/copyDirectory")
    public CommonResult<List<ListResult>> copyDirectory(@RequestParam(value = "bucketName") String bucketName,
                                                        @RequestParam(value = "sourceDir") String sourceDir,
                                                        @RequestParam(value = "destBucketName") String destBucketName,
                                                        @RequestParam(value = "destDir") String destDir) {
        List<ListResult> objectExists = utils.copyObjectDir(bucketName, sourceDir, destBucketName, destDir);
        return CommonResult.success(objectExists);
    }

    // 复制桶
    @PostMapping("/copyBucket")
    public CommonResult<List<ListResult>> copyBucket(@RequestParam(value = "bucketName") String bucketName,
                                                     @RequestParam(value = "destBucketName") String destBucketName) {
        List<ListResult> objectExists = utils.copyBucket(bucketName, destBucketName);
        return CommonResult.success(objectExists);
    }

    // 上传文件1
    @PostMapping("/uploadFile1")
    public CommonResult<String> uploadFile1(@RequestParam(value = "bucketName") String bucketName,
                                            @RequestParam(value = "objectName") String objectName,
                                            @RequestPart(value = "file") MultipartFile file) {
        return utils.uploadFile(bucketName, objectName, file) ? CommonResult.success(objectName) : CommonResult.failed(objectName);
    }

    // 上传文件2
    @PostMapping("/uploadFile2")
    public CommonResult<String> uploadFile2(@RequestParam(value = "bucketName") String bucketName,
                                            @RequestParam(value = "objectName") String objectName,
                                            @RequestPart(value = "file") MultipartFile file) {
        try (InputStream inputStream = file.getInputStream()) {
            return utils.uploadFile(bucketName, objectName, inputStream) ? CommonResult.success(objectName) : CommonResult.failed(objectName);
        } catch (IOException e) {
            return CommonResult.failed(objectName, e.getMessage());
        }
    }

    // 上传文件3
    @PostMapping("/uploadFile3")
    public CommonResult<String> uploadFile3(@RequestParam(value = "bucketName") String bucketName,
                                            @RequestParam(value = "objectName") String objectName) {
        String decodeFilePath = URLUtil.decode("C:%5CUsers%5C54915%5CDesktop%5C%E6%96%87%E4%BB%B6-14MB.bin", CharsetUtil.UTF_8);
        return utils.uploadFile(bucketName, objectName, decodeFilePath) ? CommonResult.success(objectName) : CommonResult.failed(objectName);
    }

    // 上传文件4
    @PostMapping("/uploadFile4")
    public CommonResult<String> uploadFile4(@RequestParam(value = "bucketName") String bucketName,
                                            @RequestParam(value = "objectName") String objectName) {
        return utils.uploadUrlFile(bucketName, objectName, "https://p3-ug-imc.byteimg.com/img/tos-cn-i-gflu06s87d/8a0bb37eaca543c6b09ca095ae547681~tplv-gflu06s87d-image.png") ? CommonResult.success(objectName) : CommonResult.failed(objectName);
    }

    /**
     * getUploadId(临时存储文职用) -> multipartUpload -> composeMultipartUpload
     */
    @GetMapping("/getUploadId")
    public CommonResult<String> getUploadId() {
        String uploadId = utils.getUploadId();
        return CommonResult.success(uploadId);
    }

    // createMinioFileInfo
    @GetMapping("/createMinioFileInfo")
    public CommonResult<FileInfo> createMinioFileInfo(@RequestParam(value = "filePath") String filePath,
                                                      @RequestParam(value = "partSize") Long partSize,
                                                      @RequestParam(value = "bucketName") String bucketName,
                                                      @RequestParam(value = "objectName") String objectName,
                                                      @RequestParam(value = "uploadId") String uploadId) {
        FileInfo fileInfo = utils.createMinioFileInfo(filePath, partSize, bucketName, objectName, uploadId);
        return fileInfo != null ? CommonResult.success(fileInfo) : CommonResult.failed(fileInfo);
    }

    // multipartUpload
    @PostMapping("/multipartUpload")
    public CommonResult<Boolean> multipartUpload(
                                                 @RequestPart(value = "fileInfo") FileInfo fileInfo,
                                                 @RequestPart(value = "files") List<MultipartFile> files,
                                                 @RequestParam(value = "MD5") Boolean MD5) {
        Boolean result = utils.multipartUpload(fileInfo, files, MD5);
        return result ? CommonResult.success(result) : CommonResult.failed(result);
    }

    // composeMultipartUpload
    @PostMapping("/composeMultipartUpload")
    public CommonResult<Boolean> composeMultipartUpload(
                                                       @RequestPart(value = "fileInfo") FileInfo fileInfo,
                                                       @RequestParam(value = "MD5") Boolean MD5) {
        Boolean result = utils.composeMultipartUpload(fileInfo, MD5);
        return result ? CommonResult.success(result) : CommonResult.failed(result);
    }
    // deleteObjectList
    @PostMapping("/deleteObjectList")
    public CommonResult<Boolean> deleteObjectList(@RequestParam(value = "bucketName") String bucketName,
                                                       @RequestBody List<String> objectNames) {
        Boolean result = utils.deleteObject(bucketName, objectNames);
        return CommonResult.success(result);
    }

    @GetMapping("/checkpointDownload")
    public void checkpointDownload(@RequestParam(value = "bucketName") String bucketName,
                                                   @RequestParam(value = "objectName") String objectName,
                                                   HttpServletRequest request, HttpServletResponse response) {
        utils.checkpointDownload(bucketName, objectName,request, response);
    }

    public static void main(String[] args) {
        // 获取url编码后的路径
        String encodeFilePath = URLUtil.encode("C:\\Users\\54915\\Desktop\\14.bin", CharsetUtil.CHARSET_UTF_8);
        System.out.println(encodeFilePath);

        // 根据路径分割文件，并返回文件md5
    }
}
