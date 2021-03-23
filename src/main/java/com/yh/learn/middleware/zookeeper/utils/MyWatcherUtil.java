package com.yh.learn.middleware.zookeeper.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

@Slf4j
public class MyWatcherUtil implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("Watch 监听事件为 {} ,监听路径为 {} , 监听类型为 {}", watchedEvent.getState(), watchedEvent.getPath(), watchedEvent.getType());
    }
}
