package com.yuanshuai.controller;


import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.URLUtil;
import com.obs.services.ObsClient;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.ObsObject;
import com.yuanshuai.api.CommonResult;
import com.yuanshuai.config.StorageConfig;
import com.yuanshuai.constants.StorageType;
import com.yuanshuai.domain.FileInfo;
import com.yuanshuai.domain.ListResult;
import com.yuanshuai.factory.StorageClientFactory;
import com.yuanshuai.factory.StorageUtilsFactory;
import com.yuanshuai.utils.OBSTool;
import com.yuanshuai.utils.S3Tool;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/obs")
public class OBSContorller {

    private final StorageConfig storageConfig;
    private final OBSTool utils;

    public OBSContorller(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        ObsClient client = StorageClientFactory.createClient(StorageType.OBS, storageConfig);
        this.utils = StorageUtilsFactory.createUtils(StorageType.OBS, client);
    }

    /**
     * getUploadId(临时存储文职用) -> multipartUpload -> composeMultipartUpload
     */
    @GetMapping("/getUploadId")
    public CommonResult<String> getUploadId(@RequestParam(value = "bucketName") String bucketName,
                                                          @RequestParam(value = "objectName") String objectName) {
        String uploadId = utils.getUploadId(bucketName, objectName);
        return CommonResult.success(uploadId);
    }


    // multipartUpload
    @PostMapping("/multipartUpload")

    public CommonResult<Boolean> multipartUpload(
                                                 @RequestPart(value = "s3FileInfo") FileInfo obsFileInfo,
                                                 @RequestPart(value = "files") List<MultipartFile> files) {
        Boolean result = utils.multipartUpload(obsFileInfo, files);
        return result ? CommonResult.success(result) : CommonResult.failed(result);
    }

    // composeMultipartUpload
    @PostMapping("/composeMultipartUpload")
    public CommonResult<Boolean> composeMultipartUpload(
                                                       @RequestPart(value = "s3FileInfo") FileInfo obsFileInfo) {
        Boolean result = utils.completeMultipartUpload(obsFileInfo);
        return result ? CommonResult.success(result) : CommonResult.failed(result);
    }
}
