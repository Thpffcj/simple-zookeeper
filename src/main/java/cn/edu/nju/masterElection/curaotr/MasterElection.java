package cn.edu.nju.masterElection.curaotr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by thpffcj on 2019/12/30.
 */
public class MasterElection {

    static String masterPath = "/curator_recipes_master_path";

    static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("thpffcj1:2181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {

        client.start();
        LeaderSelector selector = new LeaderSelector(client, masterPath, new LeaderSelectorListenerAdapter() {

            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println("成为Master角色");
                Thread.sleep(3000);
                System.out.println("完成Master操作，释放Master权利");
            }
        });

        selector.autoRequeue();
        selector.start();
        Thread.sleep(10000);
    }
}
