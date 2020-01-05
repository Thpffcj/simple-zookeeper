package cn.edu.nju.distributedBarrier.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
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
 * 双栏栅：DistributedDoubleBarrier，双栏栅允许客户端在计算的开始和结束时同步。当足够的进程加入到双栏栅时，进程开始计算，当计算
 * 完成时离开栏栅
 */
public class DistributedDoubleBarrierDemo {

    private static final int QTY = 5;
    private static final String PATH = "/examples/barrier";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer();

        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();

        ExecutorService service = Executors.newFixedThreadPool(QTY);

        for (int i = 0; i < QTY; i++) {
            // memberQty是成员数量
            final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, PATH, QTY);
            final int index = i;

            Callable<Void> task = () -> {
                Thread.sleep((long) (3 * Math.random()));
                System.out.println("Client：" + index + " enters");
                // 当enter()方法被调用时，成员被阻塞，直到所有的成员都调用了enter()方法
                barrier.enter();
                System.out.println("Client：" + index + " begins");
                Thread.sleep((long) (3000 * Math.random()));
                // leave()方法被调用时，它也阻塞调用线程，直到所有的成员都调用了leave()方法
                // 就像百米赛跑比赛，发令枪响，所有的运动员开始跑，等所有的运动员跑过终点线，比赛才结束
                barrier.leave();
                System.out.println("Client：" + index + " left");
                return null;
            };
            service.submit(task);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);

        CloseableUtils.closeQuietly(server);
    }
}
