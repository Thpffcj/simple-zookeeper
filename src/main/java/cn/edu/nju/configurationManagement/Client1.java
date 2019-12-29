package cn.edu.nju.configurationManagement;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.concurrent.CountDownLatch;

/**
 * Created by thpffcj on 2019/12/28.
 */
public class Client1 {

    public CuratorFramework client = null;
    public static final String zkServerPath = "thpffcj1:2181";

    public Client1() {
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace("workspace").build();
        client.start();
    }

    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public final static String CONFIG_NODE_PATH = "/super/thpffcj";
    public final static String SUB_PATH = "/redis-config";
    public static CountDownLatch countDown = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {

        Client1 cto = new Client1();
        System.out.println("client1 启动成功...");


    }
}
