package com.yh.learn.middleware.curator.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class CuratorInstance {

    @Value("${zookeeper.url}")
    private String zkUrl;

    @Bean
    public CuratorFramework curatorFramework(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
//        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkUrl, 5000, 3000, retryPolicy);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder().connectString(zkUrl)
                .sessionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .namespace("base")
                .build();
        return zkClient;
    }
}
