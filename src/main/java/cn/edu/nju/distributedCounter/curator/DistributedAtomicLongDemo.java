package cn.edu.nju.distributedCounter.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/1/4.
 *
 * DistributedAtomicInteger和SharedCount计数范围是一样的,都是int类型，但是DistributedAtomicInteger和
 * DistributedAtomicLong和上面的计数器的实现有显著的不同，它首先尝试使用乐观锁的方式设置计数器，如果不成功(比如期间计数器已经
 * 被其它client更新了)，它使用InterProcessMutex方式来更新计数值。
 */
public class DistributedAtomicLongDemo {

    private static final int QTY = 5;
    private static final String PATH = "/examples/counter";

    public static void main(String[] args) throws Exception {

        TestingServer server = new TestingServer();

        // 重试策略，初试时间1秒，重试3次
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        // 通过工厂创建Curator
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(policy).build();
        // 开启连接
        client.start();

        ExecutorService service = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; i++) {
            final DistributedAtomicLong count = new DistributedAtomicLong(
                    client, PATH, new RetryNTimes(10, 1000));

            Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        AtomicValue<Long> value = count.increment();
                        System.out.println("操作是否成功: " + value.succeeded());
                        if (value.succeeded()){
                            System.out.println("操作成功: 操作前的值为： " + value.preValue() + " 操作后的值为： " + value.postValue());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
            service.submit(task);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);

        CloseableUtils.closeQuietly(server);
    }
}
