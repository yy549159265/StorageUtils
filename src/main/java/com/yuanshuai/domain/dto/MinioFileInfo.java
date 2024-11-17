package com.yuanshuai.domain.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MinioFileInfo {

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
     * 需要上下传文件uid
     */
    private String uploadId;

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


