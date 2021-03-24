package com.yh.learn.middleware.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import com.yh.learn.middleware.zookeeper.utils.ZookeeperUtil;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@SpringBootTest
public class ZookeeperTest {

    @Resource
    private ZookeeperUtil zookeeperUtil;

    private static String path = "/yh";

    @Test
    public void existNode(){
        Stat exists = zookeeperUtil.exists(path, false);
        log.error("判断路径下 {} 是否存在, stat 为 {}", path, exists);
    }

    @Test
    public void createNode(){
        String node = zookeeperUtil.createNode(path, "data0");
        log.error("创建节点 {}，请求返回体为 {}", path, node);
        existNode();
    }

    @Test
    public void deleteNode(){
        zookeeperUtil.deleteNode(path, -1);
        log.error("节点 {} 下 的节点已被删除", path);
        existNode();
    }

    @Test
    public void updateNode(){
        Stat stat = zookeeperUtil.updateNode(path, "data1", -1);
        log.error("创建节点 {} ，更新结果为 {}", path, stat);
    }

    @Test
    public void createTreeNode(){
        String node = zookeeperUtil.createNode("/yh/test", "data0");
        log.error("创建节点 {}，请求返回体为 {}", "/yh/test", node);
    }

    @Test
    public void getChildren(){
        List<String> children = zookeeperUtil.getChildren(path);
        log.error("节点 {} 下 的子节点为 {}", path, children);
    }
}
