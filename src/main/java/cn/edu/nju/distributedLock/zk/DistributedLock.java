package cn.edu.nju.distributedLock.zk;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created by thpffcj on 2019/12/23.
 */
public class DistributedLock implements Lock, Watcher {

    private ZooKeeper zooKeeper;

    private String parentPath;

    private CountDownLatch latch = new CountDownLatch(1);

    private static ThreadLocal<String> currentNodePath = new ThreadLocal<String>();

    public DistributedLock(String url, int sessionTimeout, String path) {

        this.parentPath = path;
        try {
            zooKeeper = new ZooKeeper(url, sessionTimeout, this);
            latch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if (tryLock()) {
            System.out.println(Thread.currentThread().getName() + "成功获取锁");
        } else {
            String myPath = currentNodePath.get();
            synchronized (myPath) {
                try {
                    myPath.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(Thread.currentThread().getName() + "等待锁完成");
            lock();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {

        String myPath = currentNodePath.get();
        try {
            if (myPath == null) {
                myPath = zooKeeper.create(parentPath + "/", "dist_lock".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                currentNodePath.set(myPath);
                System.out.println(Thread.currentThread().getName() + "已经创建" + myPath);
            }

            final String currentPath = myPath;
            List<String> allNodes = zooKeeper.getChildren(parentPath, false);
            Collections.sort(allNodes);

            String nodeName = currentPath.substring((parentPath + "/").length());
            if (allNodes.get(0).equals(nodeName)) {
                System.out.println(Thread.currentThread().getName() + "tryLock 成功");
                return true;
            } else {
                // 监听上一个节点，防止羊群效应
                String targetNodeName = "";
                for (String node : allNodes) {
                    if (nodeName.equals(node)) {
                        break;
                    } else {
                        targetNodeName = node;
                    }
                }
                targetNodeName = parentPath + "/" + targetNodeName;

                System.out.println(Thread.currentThread().getName() + "需要等待删除节点" + targetNodeName);

                zooKeeper.exists(targetNodeName, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("收到事件：" + event);
                        if (event.getType() == Event.EventType.NodeDeleted) {
                            synchronized (currentPath) {
                                currentPath.notify();
                            }
                            System.out.println(Thread.currentThread().getName() + "获取到NodeDeleted通知，重新尝试获取锁");
                        }
                    }
                });
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        String myPath = currentNodePath.get();
        if (myPath != null) {
            System.out.println(Thread.currentThread().getName() + "释放锁");
            currentNodePath.remove();
            try {
                zooKeeper.delete(myPath, -1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("收到事件：" + event);
        if (event.getType() == Event.EventType.None
                && event.getState() == Event.KeeperState.SyncConnected) {
            latch.countDown();
            System.out.println("已经连接成功");
        }
    }
}
