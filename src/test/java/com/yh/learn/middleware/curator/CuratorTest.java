package com.yh.learn.middleware.curator;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

@Slf4j
@SpringBootTest
public class CuratorTest {

    @Resource
    private CuratorFramework client;

    @Test
    public void createEphemeralAndParentIfNeeded(){
        try {
            client.start();
            String s = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("/com/yh/learn/base/zk", "hello world".getBytes());
            log.info("create temp node success {}", s);

            Stat stat = new Stat();
            log.info(new String(client.getData().storingStatIn(stat).forPath("/com/yh/learn/base/zk")));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Test
    public void testZKCache(){
        client.start();
        try {
            String s = client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("/com/yh/learn/base/zk", "hello".getBytes());
            log.info("create success => {}", s);
            final NodeCache cache = new NodeCache(client, "/com/yh/learn/base/zk", false);
            cache.start(true);
            cache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    log.info("node data update, new data is [{}]", new String(cache.getCurrentData().getData()));
                }
            });
            Stat stat = client.setData().forPath("/com/yh/learn/base/zk", "are you ok".getBytes());
            log.info("update success => {}", stat.toString());
            Thread.sleep(100);
            client.delete().deletingChildrenIfNeeded().forPath("/com/yh/learn/base/zk");
            Thread.sleep(10000);
        } catch (Exception e) {
            log.error("error => {}", e.getMessage());
        }
    }

    @Test
    public void testSync(){
        for(int i = 0;i < 10;i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                    String orderNo = sdf.format(new Date());
                    log.info("new order number is => {}", orderNo);
                }
            }).start();
        }
    }

    @Test
    public void testZKLock(){
        String path = "/lock";
        CuratorFramework zkClient = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .namespace("base")
                .build();
        zkClient.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}
        final InterProcessMutex lock = new InterProcessMutex(zkClient, path);
        final CountDownLatch down = new CountDownLatch(1);
        for(int i = 0;i < 10;i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        down.await();
                        lock.acquire();
                    } catch (Exception e) {
                        log.error("multi thread error", e);
                    }
                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                    String orderNo = sdf.format(new Date());
                    log.info("new order number is => {}", orderNo);
                    try {
                        lock.release();
                    } catch (Exception e) {
                        log.error("release lock error", e);
                    }
                }
            }).start();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}
        down.countDown();
    }
}
