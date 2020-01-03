package cn.edu.nju.distributedLock.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/1/3.
 *
 * 可重入锁：InterProcessMutex(CuratorFramework client, String path)
 *
 * 通过acquire()获得锁，并提供超时机制；通过release()释放锁。makeRevocable(RevocationListener<T> listener)定义了可协
 * 商的撤销机制，当别的进程或线程想让你释放锁时，listener会被调用。如果请求撤销当前的锁，可以调用
 * attemptRevoke(CuratorFramework client, String path)。
 */
public class InterProcessMutexDemo {

    private InterProcessMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    public InterProcessMutexDemo(
            CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        this.lock = new InterProcessMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        try {
            System.out.println(clientName + " get the lock");
            resource.use(); // access resource exclusively
        } finally {
            System.out.println(clientName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
        }
    }
}
