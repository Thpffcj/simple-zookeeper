package cn.edu.nju.starter.curator;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by thpffcj on 2019/12/28.
 */
public class MyWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
        System.out.println("触发watcher，节点路径为：" + event.getPath());
    }
}
