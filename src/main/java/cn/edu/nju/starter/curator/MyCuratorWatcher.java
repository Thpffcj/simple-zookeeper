package cn.edu.nju.starter.curator;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * Created by thpffcj on 2019/12/28.
 */
public class MyCuratorWatcher implements CuratorWatcher {

    @Override
    public void process(WatchedEvent event) throws Exception {
        System.out.println("触发watcher，节点路径为：" + event.getPath());
    }
}
