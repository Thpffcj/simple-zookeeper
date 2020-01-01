package cn.edu.nju.starter.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by thpffcj on 2019/12/31.
 *
 * NodeCache：监听节点的新增、修改操作
 */
public class CuratorWatcher1 {

    private static final String CONNECT_ADDR = "thpffcj1:2181";
    private static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {

        RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(policy).build();
        curator.start();

        // 最后一个参数表示是否进行压缩
        NodeCache cache = new NodeCache(curator, "/super", false);
        cache.start(true);

//        cache.getListenable().addListener(new NodeCacheListener() {
//            @Override
//            public void nodeChanged() throws Exception {
//                System.out.println("路径：" + cache.getCurrentData().getPath());
//                System.out.println("数据：" + new String(cache.getCurrentData().getData()));
//                System.out.println("状态：" + cache.getCurrentData().getStat());
//            }
//        });

        // 只会监听节点的创建和修改，删除不会监听
        cache.getListenable().addListener(() -> {
            System.out.println("路径：" + cache.getCurrentData().getPath());
            System.out.println("数据：" + new String(cache.getCurrentData().getData()));
            System.out.println("状态：" + cache.getCurrentData().getStat());
        });

        curator.create().forPath("/super", "1234".getBytes());
        Thread.sleep(1000);
        curator.setData().forPath("/super", "5678".getBytes());
        Thread.sleep(1000);
        // TODO 报错？也回调了监听？
        curator.delete().forPath("/super");
        Thread.sleep(5000);
        curator.close();
    }
}
