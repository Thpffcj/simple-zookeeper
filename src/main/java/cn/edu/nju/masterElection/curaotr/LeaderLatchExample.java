package cn.edu.nju.masterElection.curaotr;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.List;

/**
 * Created by thpffcj on 2020/1/2.
 *
 * 一旦选举出Leader，除非有客户端挂掉重新触发选举，否则不会交出领导权
 */
public class LeaderLatchExample {

    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/examples/leader";

    public static void main(String[] args) throws Exception {

        System.out.println("创建 " + CLIENT_QTY
                + " 个客户端, 公平的参与leader选举，成为leader后，将一直占用此leader权。直到主动放弃leader权");

        List<CuratorFramework> clients = Lists.newArrayList();
        List<LeaderLatch> leaderLatches = Lists.newArrayList();

        TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < CLIENT_QTY; i++) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(
                        server.getConnectString(),
                        new ExponentialBackoffRetry(20000, 3));
                clients.add(client);

                LeaderLatch latch = new LeaderLatch(client, PATH, "Client：" + i);
                latch.addListener(new LeaderLatchListener() {

                    // 当实例成为leader后，会调用isLeader()方法，之后除非此实例连接不到zookeeper，否侧将一直占着leader权
                    @Override
                    public void isLeader() {
                        System.out.println("I am Leader");
                    }

                    // 抢主失败时触发
                    @Override
                    public void notLeader() {
                        System.out.println("I am not Leader");
                    }
                });
                leaderLatches.add(latch);

                client.start();
                latch.start();
            }

            Thread.sleep(5000);
            LeaderLatch currentLeader = null;
            for (LeaderLatch latch : leaderLatches) {
                if (latch.hasLeadership()) {
                    currentLeader = latch;
                }
            }
            System.out.println("current leader is " + currentLeader.getId());
            System.out.println("release the leader " + currentLeader.getId());
            // 一旦不使用LeaderLatch了，必须调用close方法。如果它是leader,会释放leadership，其它的参与者将会选举一个leader
            currentLeader.close();

            Thread.sleep(5000);

            for (LeaderLatch latch : leaderLatches) {
                if (latch.hasLeadership()) {
                    currentLeader = latch;
                }
            }
            System.out.println("current leader is " + currentLeader.getId());
            System.out.println("release the leader " + currentLeader.getId());

        } finally {
            for (LeaderLatch latch : leaderLatches) {
                if (LeaderLatch.State.CLOSED != latch.getState()) {
                    CloseableUtils.closeQuietly(latch);
                }
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            CloseableUtils.closeQuietly(server);
        }
    }
}
