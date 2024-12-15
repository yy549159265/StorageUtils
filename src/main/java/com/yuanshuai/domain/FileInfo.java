package com.yuanshuai.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileInfo {

    /**
     * 桶名称
     */
    private String bucketName;

    /**
     * 文件名称
     */
    private String objectName;

    /**
     * 原始文件md5值
     */
    private String fileMd5;

    /**
     * 需要上下传文件id(s3使用)
     */
    private String uploadId;

    /**
     * 分片上传的url
     */
    private String uploadUrl;


    /**
     * 总分片
     */
    private String totalNum;

    /**
     * 分片信息
     */
    private List<PartInfo> parts;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PartInfo {
        /**
         * 分片文件md5值
         */
        private String partMd5;

        /**
         * 当前分片
         */
        private String currentNum;
    }

}


