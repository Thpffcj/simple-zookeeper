package cn.edu.nju.masterElection.curator;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created by thpffcj on 2020/1/2.
 *
 * 所有存活的客户端不间断的轮流做Leader
 */
public class LeaderSelectorDemo {

    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/examples/leader";

    public static void main(String[] args) throws Exception {

        System.out.println("创建 " + CLIENT_QTY + " 个客户端, 公平的参与leader选举，成为leader后，会等待一个随机的时" +
                "间（几秒中），之后释放leader权，所有实例重新进行leader选举。");

        List<CuratorFramework> clients = Lists.newArrayList();
        List<SelectorClient> selectorClients = Lists.newArrayList();

        TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < CLIENT_QTY; i++) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),
                        new ExponentialBackoffRetry(1000, 3));
                clients.add(client);

                SelectorClient selectorClient = new SelectorClient(client, PATH, "Client：" + i);
                selectorClients.add(selectorClient);

                client.start();
                selectorClient.start();
            }

            System.out.println("按 enter/return 来退出\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } finally {
            System.out.println("关闭程序...");

            for (SelectorClient selectorClient : selectorClients) {
                CloseableUtils.closeQuietly(selectorClient);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }

            CloseableUtils.closeQuietly(server);
        }
    }
}
