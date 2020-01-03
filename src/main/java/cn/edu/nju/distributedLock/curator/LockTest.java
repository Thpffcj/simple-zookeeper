package cn.edu.nju.distributedLock.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/1/3.
 */
public class LockTest {

    private static final int QTY = 5;
    private static final int REPETITIONS = QTY * 10;
    private static final String PATH = "/examples/locks";

    /**
     * 生成5个client， 每个client重复执行10次，请求锁–访问资源–释放锁的过程。
     */
    public static void main(String[] args) throws Exception {
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService executor = Executors.newFixedThreadPool(QTY);
        final TestingServer server = new TestingServer();

        try {
            for (int i = 0; i < QTY; i++) {
                final int index = i;
                Callable<Void> task = () -> {
                    CuratorFramework client = CuratorFrameworkFactory.newClient(
                            server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                    client.start();

                    try {
                        /**
                         * 锁是随机的被每个实例排他性的使用。
                         * 既然是可重用的，你可以在一个线程中多次调用acquire(),在线程拥有锁时它总是返回true。
                         * 你不应该在多个线程中用同一个InterProcessMutex， 你可以在每个线程中都生成一个新的InterProcessMutex实例，
                         * 它们的path都一样，这样它们可以共享同一个锁
                         */
                        final InterProcessMutexDemo example = new InterProcessMutexDemo(
                                client, PATH, resource, "Client：" + index);

                        /**
                         * 运行后发现，有且只有一个client成功获取第一个锁(第一个acquire()方法返回true)，然后它自己阻塞在第
                         * 二个acquire()方法，获取第二个锁超时；其他所有的客户端都阻塞在第一个acquire()方法超时并且抛出异常。
                         * 这样也就验证了InterProcessSemaphoreMutex实现的锁是不可重入的。
                         */
//                        final InterProcessSemaphoreMutexDemo example = new InterProcessSemaphoreMutexDemo(
//                                client, PATH, resource, "Client " + index);

                        for (int j = 0; j < REPETITIONS; j++) {
                            example.doWork(10, TimeUnit.SECONDS);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        CloseableUtils.closeQuietly(client);
                    }
                    return null;
                };
                executor.submit(task);
            }
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } finally {
            CloseableUtils.closeQuietly(server);
        }

    }
}
