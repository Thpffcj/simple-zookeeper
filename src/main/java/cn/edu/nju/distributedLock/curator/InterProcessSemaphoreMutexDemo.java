package cn.edu.nju.distributedLock.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/1/3.
 *
 * 这个锁和上面的InterProcessMutex相比，就是少了Reentrant的功能，也就意味着它不能在同一个线程中重入
 */
public class InterProcessSemaphoreMutexDemo {

    private InterProcessSemaphoreMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    public InterProcessSemaphoreMutexDemo(
            CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        this.lock = new InterProcessSemaphoreMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到互斥锁");
        }
        System.out.println(clientName + " 已获取到互斥锁");

        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到互斥锁");
        }
        System.out.println(clientName + " 再次获取到互斥锁");

        try {
            System.out.println(clientName + " get the lock");
            resource.use(); // access resource exclusively
        } finally {
            System.out.println(clientName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
            lock.release(); // 获取锁几次 释放锁也要几次
        }
    }
}
