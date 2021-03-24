package com.yh.learn.middleware.zookeeper.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Configuration
public class ZookeeperUtil {

    @Resource
    ZooKeeper zooKeeper;

    public Stat exists(String path, boolean needWatch) {
        Stat stat = null;
        try {
            stat = zooKeeper.exists(path, needWatch);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        return stat;
    }

    /**
     * 创建持久化节点
     * @param path
     * @param data
     * @return
     */
    public String createNode(String path, String data) {
        String b = null;
        try {
            b = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        return b;
    }

    /**
     * 基于版本进行原子操作的加锁，数据版本从0开始更新
     * @param path
     * @param data
     * @param version 为-1时表示忽略版本更新；否则，版本不匹配时，更新失败
     * @return
     */
    public Stat updateNode(String path, String data, int version) {
        Stat stat = null;
        try {
            stat = zooKeeper.setData(path, data.getBytes(), version);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        return stat;
    }

    public void deleteNode(String path, int version) {
        try {
            zooKeeper.delete(path, version);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public List<String> getChildren(String path) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        return children;
    }
}
