package cn.edu.nju.distributedBarrier.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/1/4.
 *
 * 分布式Barrier是这样一个类：它会阻塞所有节点上的等待进程，知道某一个被满足，然后所有的节点继续执行。比如赛马比赛中，等赛马陆续来到
 * 起跑线前，一声令下，所有的赛马都飞奔而出
 */
public class DistributedBarrierDemo {

    private static final int QTY = 5;
    private static final String PATH = "/examples/barrier";

    public static void main(String[] args) throws Exception {

        TestingServer server = new TestingServer();
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();

        ExecutorService service = Executors.newFixedThreadPool(QTY);

        DistributedBarrier controlBarrier = new DistributedBarrier(client, PATH);
        // 首先需要调用setBarrier()方法设置栏栅，它将阻塞在它上面等待的线程
        controlBarrier.setBarrier();

        for (int i = 0; i < QTY; i++) {
            final DistributedBarrier barrier = new DistributedBarrier(client, PATH);
            final int index = i;
            Callable<Void> task = () -> {
                Thread.sleep((long) (3 * Math.random()));
                System.out.println("Client：" + index + " waits on Barrier");
                // 然后需要阻塞的线程调用waitOnBarrier()方法等待放行条件
                barrier.waitOnBarrier();
                System.out.println("Client：" + index + " begins");
                return null;
            };
            service.submit(task);
        }
        Thread.sleep(5000);
        System.out.println("all Barrier instances should wait the condition");
        // 当条件满足时调用removeBarrier()方法移除栏栅，所有等待的线程将继续执行
        controlBarrier.removeBarrier();

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);

        CloseableUtils.closeQuietly(server);
    }
}
