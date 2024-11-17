package com.yuanshuai.domain.minio;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MinioListResult {

    private String Name;
    private Boolean isSuccess;
}
