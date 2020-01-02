package cn.edu.nju.masterElection.curaotr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by thpffcj on 2020/1/2.
 */
public class SelectorClient extends LeaderSelectorListenerAdapter implements Closeable {

    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public SelectorClient(CuratorFramework client, String path, String name) {
        this.name = name;

        // 利用一个给定的路径创建一个leader selector
        // 执行leader选举的所有参与者对应的路径必须一样
        // 本例中SelectorClient也是一个LeaderSelectorListener，但这不是必须的。
        leaderSelector = new LeaderSelector(client, path, this);

        // 在大多数情况下，我们会希望一个selector放弃leader后还要重新参与leader选举
        leaderSelector.autoRequeue();
    }

    /**
     * 启动当前实例参与leader选举
     * @throws IOException
     */
    public void start() throws IOException {
        // leader选举是在后台处理的，所以这个方法会立即返回
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    /**
     * 当前实例成为leader时，会执行下面的方法，这个方法执行结束后，当前实例就自动释放leader了，所以在想放弃leader前此方法不能结束
     * @param client
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {

        final int waitSeconds = (int) (5 * Math.random()) + 1;
        System.out.println(name + " 现在是leader了，持续成为leader " + waitSeconds + " 秒.");
        System.out.println(name + " 之前已经成为了 " + leaderCount.getAndIncrement() + " 次leader.");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            System.err.println(name + " was interrupted.");
            Thread.currentThread().interrupt();
        } finally {
            System.out.println(name + " 释放leader权.\n");
        }
    }
}
