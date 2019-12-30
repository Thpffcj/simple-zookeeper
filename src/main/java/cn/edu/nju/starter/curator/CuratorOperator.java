package cn.edu.nju.starter.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;

/**
 * Created by thpffcj on 2019/12/27.
 */
public class CuratorOperator {

    public CuratorFramework client = null;
    public static final String zkServerPath = "thpffcj1:2181";

    /**
     * 实例化zk客户端
     */
    public CuratorOperator() {

        /**
         * 同步创建zk示例，原生api是异步的
         *
         * curator链接zookeeper的策略:ExponentialBackoffRetry
         * baseSleepTimeMs：初始sleep的时间
         * maxRetries：最大重试次数
         * maxSleepMs：最大重试时间
         */
//		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * curator链接zookeeper的策略:RetryNTimes
         * n：重试的次数
         * sleepMsBetweenRetries：每次重试间隔的时间
         */
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        /**
         * curator链接zookeeper的策略:RetryOneTime
         * sleepMsBetweenRetry:每次重试间隔的时间
         */
//		RetryPolicy retryPolicy2 = new RetryOneTime(3000);

        /**
         * 永远重试，不推荐使用
         */
//		RetryPolicy retryPolicy3 = new RetryForever(retryIntervalMs);

        /**
         * curator链接zookeeper的策略:RetryUntilElapsed
         * maxElapsedTimeMs:最大重试时间
         * sleepMsBetweenRetries:每次重试间隔
         * 重试时间超过maxElapsedTimeMs后，就不再重试
         */
//		RetryPolicy retryPolicy4 = new RetryUntilElapsed(2000, 3000);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace("workspace").build();
        client.start();
    }

    /**
     * 关闭zk客户端连接
     */
    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {

        // 实例化
        CuratorOperator cto = new CuratorOperator();
        boolean isZkCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭"));

        // 创建节点
        String nodePath = "/super/thpffcj";
//        cto.createNode(nodePath);

        // 更新节点数据
        cto.updateNode(nodePath);

        // 删除节点
//		cto.deleteNode(nodePath);

        // 读取节点数据
//		Stat stat = new Stat();
//		byte[] data = cto.client.getData().storingStatIn(stat).forPath(nodePath);
//		System.out.println("节点" + nodePath + "的数据为: " + new String(data));
//		System.out.println("该节点的版本号为: " + stat.getVersion());

        // 查询子节点
//		List<String> childNodes = cto.client.getChildren().forPath(nodePath);
//		System.out.println("开始打印子节点：");
//		for (String s : childNodes) {
//			System.out.println(s);
//		}

        // 判断节点是否存在，如果不存在则为空
//		Stat statExist = cto.client.checkExists().forPath(nodePath + "/a");
//		System.out.println(statExist);

        // TODO watcher事件，当使用usingWatcher的时候，监听只会触发一次，监听完毕后就销毁
//		cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
//		cto.client.getData().usingWatcher(new MyWatcher()).forPath(nodePath);

        // NodeCache: 监听数据节点的变更，会触发事件
        cto.setNodeCache(nodePath);


        Thread.sleep(100000);

        cto.closeZKClient();
        boolean isZkCuratorStarted2 = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted2 ? "连接中" : "已关闭"));
    }

    public void createNode(String nodePath) throws Exception {
        byte[] data = "superme".getBytes();
		// creatingParentsIfNeeded()方法的意思是如果父节点不存在，则在创建节点的同时创建父节点
		client.create().creatingParentsIfNeeded()
			.withMode(CreateMode.PERSISTENT)
			.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
			.forPath(nodePath, data);
    }

    public void updateNode(String nodePath) throws Exception {
        byte[] newData = "batman".getBytes();
        client.setData().withVersion(0).forPath(nodePath, newData);
    }

    public void deleteNode(String nodePath) throws Exception {
        client.delete()
                .guaranteed()					// 如果删除失败，那么在后端还是继续会删除，直到成功
                .deletingChildrenIfNeeded()	    // 如果有子节点，就删除
                .withVersion(1)
                .forPath(nodePath);
    }

    /**
     * 为节点添加watcher
     * Zookeeper原生支持通过注册Watcher来进行事件监听，但是开发者需要反复注册(Watcher只能单次注册单次使用)。Cache
     * 是Curator中对事件监听的包装，可以看作是对事件监听的本地缓存视图，能够自动为开发者处理反复注册监听。Curator提供了
     * 三种Watcher(Cache)来监听结点的变化。
     *
     * @param nodePath
     * @throws Exception
     */
    public void setNodeCache(String nodePath) throws Exception {

        // NodeCache: 监听数据节点的变更，会触发事件
        final NodeCache nodeCache = new NodeCache(client, nodePath);
        // buildInitial : 初始化的时候获取node的值并且缓存
        nodeCache.start(true);
        if (nodeCache.getCurrentData() != null) {
            System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
        } else {
            System.out.println("节点初始化数据为空...");
        }
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            public void nodeChanged() throws Exception {
                if (nodeCache.getCurrentData() == null) {
                    System.out.println("空");
                    return;
                }
                String data = new String(nodeCache.getCurrentData().getData());
                System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
            }
        });
    }

    public final static String ADD_PATH = "/super/thpffcj/d";

    public void setChildrenCache(String nodePath) throws Exception {
        // 为子节点添加watcher
        // PathChildrenCache: 监听数据节点的增删改，会触发事件
        String childNodePathCache = nodePath;
        // cacheData: 设置缓存节点的数据状态
        final PathChildrenCache childrenCache = new PathChildrenCache(
                client, childNodePathCache, true);

        /**
         * StartMode: 初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(StartMode.POST_INITIALIZED_EVENT);

        List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表：");
        for (ChildData cd : childDataList) {
            String childData = new String(cd.getData());
            System.out.println(childData);
        }

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                    System.out.println("子节点初始化ok...");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    String path = event.getData().getPath();
                    if (path.equals(ADD_PATH)) {
                        System.out.println("添加子节点:" + event.getData().getPath());
                        System.out.println("子节点数据:" + new String(event.getData().getData()));
                    } else if (path.equals("/super/thpffcj/e")) {
                        System.out.println("添加不正确...");
                    }
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    System.out.println("删除子节点:" + event.getData().getPath());
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("修改子节点路径:" + event.getData().getPath());
                    System.out.println("修改子节点数据:" + new String(event.getData().getData()));
                }
            }
        });
    }
}
