package com.yuanshuai.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Configuration
@ConfigurationProperties(prefix = "osc-config")
public class OscConfig {

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucketName;
    private String region;

}
