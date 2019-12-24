package cn.edu.nju;

/**
 * Created by thpffcj on 2019/12/23.
 */
public class LockTest {

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread() {
                public void run() {
                    DistributedLock distributedLock = new DistributedLock("localhost:2181", 3000, "/lock");
                    try {
                        distributedLock.lock();
                        System.out.println(Thread.currentThread().getName() + "开始执行");
                        Thread.sleep(100);
                        System.out.println(Thread.currentThread().getName() + "执行完成");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        distributedLock.unlock();
                    }
                }
            }.start();
        }
    }
}