package com.yh.learn.middleware.zookeeper.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Configuration
@Slf4j
public class ZookeeperConfig {

    @Value("${zookeeper.url}")
    private String zkUrl;

    @Bean
    public ZooKeeper zkClient() {
        ZooKeeper zkClient = null;
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            zkClient = new ZooKeeper(zkUrl, 50000, watchedEvent -> {
                switch (watchedEvent.getState()) {
                    case Closed:
                        log.error("zookeeper 已经关闭...，连接状态为 {}", watchedEvent.getState());
                        break;
                    case Expired:
                        log.error("zookeeper 会话已超时，连接状态为 {}", watchedEvent.getState());
                        break;
                    case AuthFailed:
                        log.error("zookeeper 权限验证失败，连接状态为 {}", watchedEvent.getState());
                        break;
                    case Disconnected:
                        log.error("与 zookeeper 服务器端连接已断开，连接状态为 {}", watchedEvent.getState());
                        break;
                    case SyncConnected:
                        log.error("与 zookeeper 建立连接，连接状态为 {}", watchedEvent.getState());
                        countDownLatch.countDown();
                        break;
                    default:
                        log.error("与 zookeeper 连接状态为 {}", watchedEvent.getState());
                }
            });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        return zkClient;
    }
}
