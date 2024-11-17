package com.yuanshuai.utils;


import com.obs.services.ObsClient;

import lombok.extern.slf4j.Slf4j;


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
            throw new RuntimeException("无法关闭连接",e);
        }
    }
}
