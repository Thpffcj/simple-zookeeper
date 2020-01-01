package cn.edu.nju.masterElection.curaotr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by thpffcj on 2019/12/31.
 *
 * 一旦选举出Leader，除非有客户端挂掉重新触发选举
 */
public class MasterElection2 {

    protected static String lockPath = "/curator_recipes_master_path/leader";
    private static final int CLIENT_QTY = 10;

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < CLIENT_QTY; i++) {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString("thpffcj1:2181")
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            client.start();

            LeaderLatch latch = new LeaderLatch(client, lockPath, "Client：" + i);
            latch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    System.out.println("I am Leader");
                }

                @Override
                public void notLeader() {
                    System.out.println("I am not Leader");
                }
            });

            latch.start();
        }

        Thread.sleep(10000);
        LeaderLatch currentLeader = null;
        for (LeaderLatch latch : examples) {
            if (latch.hasLeadership()) {
                currentLeader = latch;
            }
        }
        System.out.println("current leader is " + currentLeader.getId());
        System.out.println("release the leader " + currentLeader.getId());
        currentLeader.close();


    }
}
