package cn.edu.nju.starter.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by thpffcj on 2019/12/31.
 *
 * create创建节点方法可选的链式项：creatingParentsIfNeeded（是否同时创建父节点）、withMode（创建的节点类型）、
 * forPath（创建的节点路径）、withACL（安全项）
 *
 * delete删除节点方法可选的链式项：deletingChildrenIfNeeded（是否同时删除子节点）、guaranteed（安全删除）、
 * withVersion（版本检查）、forPath（删除的节点路径）
 *
 * inBackground绑定异步回调方法。比如在创建节点时绑定一个回调方法，该回调方法可以输出服务器的状态码以及服务器的事件类型等信息，
 * 还可以加入一个线程池进行优化操作
 */
public class CuratorHelloWorld {

    private static final String CONNECT_ADDR = "thpffcj1:2181";
    private static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        // 重试策略，初试时间1秒，重试10次
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 10);

        // 通过工厂创建Curator
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(policy).build();

        // 开启连接
        curator.start();

        ExecutorService executor = Executors.newCachedThreadPool();

//        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
//                .inBackground(new BackgroundCallback() {
//                    /**
//                     * @param client 当前服务端实例
//                     * @param event 服务端事件
//                     * @throws Exception
//                     */
//                    @Override
//                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
//                        System.out.println("Code：" + event.getResultCode());
//                        System.out.println("Type：" + event.getType());
//                        System.out.println("Path：" + event.getPath());
//                    }
//                }, executor).forPath("/super/c1", "c1内容".getBytes());

        /**
         * 创建节点
         * creatingParentsIfNeeded()方法的意思是如果父节点不存在，则在创建节点的同时创建父节点
         * withMode()方法指定创建的节点类型，跟原生的Zookeeper API一样，不设置默认为PERSISTENT类型
         */
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .inBackground((framework, event) -> {  // 添加回调
                    System.out.println("Code：" + event.getResultCode());
                    System.out.println("Type：" + event.getType());
                    System.out.println("Path：" + event.getPath());
                }, executor).forPath("/super/c1", "c1内容".getBytes());

        // 为了能够看到回调信息
        Thread.sleep(5000);

        // 获取节点数据
        String data = new String(curator.getData().forPath("/super/c1"));
        System.out.println(data);

        // 判断指定节点是否存在
        Stat stat = curator.checkExists().forPath("/super/c1");
        System.out.println(stat);

        // 更新节点数据
        curator.setData().forPath("/super/c1", "c1新内容".getBytes());
        data = new String(curator.getData().forPath("/super/c1"));
        System.out.println(data);

        // 获取子节点
        List<String> children = curator.getChildren().forPath("/super");
        for(String child : children) {
            System.out.println(child);
        }

        // 放心的删除节点，deletingChildrenIfNeeded()方法表示如果存在子节点的话，同时删除子节点
        curator.delete().guaranteed().deletingChildrenIfNeeded().forPath("/super");
        curator.close();
    }
}
