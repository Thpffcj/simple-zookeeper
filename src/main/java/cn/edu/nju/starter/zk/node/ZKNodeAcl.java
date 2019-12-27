package cn.edu.nju.starter.zk.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import cn.edu.nju.starter.utils.AclUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * Created by thpffcj on 2019/12/26.
 *
 * zookeeper 操作节点acl演示
 */
public class ZKNodeAcl implements Watcher {

    private ZooKeeper zookeeper = null;

    public static final String zkServerPath = "thpffcj1:2181";
    public static final Integer timeout = 5000;

    public ZKNodeAcl() {
    }

    public ZKNodeAcl(String connectString) {
        try {
            zookeeper = new ZooKeeper(connectString, timeout, new ZKNodeAcl());
        } catch (IOException e) {
            e.printStackTrace();
            if (zookeeper != null) {
                try {
                    zookeeper.close();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public void createZKNode(String path, byte[] data, List<ACL> acls) {

        String result = "";
        try {
            /**
             * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
             * 参数：
             * path：创建的路径
             * data：存储的数据的byte[]
             * acl：控制权限策略
             * 			Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             * 			CREATOR_ALL_ACL --> auth:user:password:cdrwa
             * createMode：节点类型, 是一个枚举
             * 			PERSISTENT：持久节点
             * 			PERSISTENT_SEQUENTIAL：持久顺序节点
             * 			EPHEMERAL：临时节点
             * 			EPHEMERAL_SEQUENTIAL：临时顺序节点
             */
            result = zookeeper.create(path, data, acls, CreateMode.PERSISTENT);
            System.out.println("创建节点：" + result + " 成功...");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        ZKNodeAcl zkServer = new ZKNodeAcl(zkServerPath);

        /**
         * ======================  创建node start  ======================
         */
        // acl 任何人都可以访问
//		zkServer.createZKNode("/aclthpffcj", "test".getBytes(), Ids.OPEN_ACL_UNSAFE);

        // 自定义用户认证访问
//		List<ACL> acls = new ArrayList<ACL>();
//		Id thpffcj1 = new Id("digest", AclUtils.getDigestUserPwd("thpffcj1:123456"));
//		Id thpffcj2 = new Id("digest", AclUtils.getDigestUserPwd("thpffcj2:123456"));
//		acls.add(new ACL(Perms.ALL, thpffcj1));
//		acls.add(new ACL(Perms.READ, thpffcj2));
//		acls.add(new ACL(Perms.DELETE | Perms.CREATE, thpffcj2));
//		zkServer.createZKNode("/aclthpffcj/testdigest", "testdigest".getBytes(), acls);

        // 注册过的用户必须通过addAuthInfo才能操作节点，参考命令行 addauth
//		zkServer.getZookeeper().addAuthInfo("digest", "thpffcj2:123456".getBytes());
//		zkServer.createZKNode("/aclthpffcj/testdigest/childtest", "childtest".getBytes(), Ids.CREATOR_ALL_ACL);
//		Stat stat = new Stat();
//		byte[] data = zkServer.getZookeeper().getData("/aclthpffcj/testdigest", false, stat);
//		System.out.println(new String(data));
//		zkServer.getZookeeper().setData("/aclthpffcj/testdigest", "now".getBytes(), 0);

        // ip方式的acl
//		List<ACL> aclsIP = new ArrayList<ACL>();
//		Id ipId1 = new Id("ip", "192.168.92.1");
//		aclsIP.add(new ACL(Perms.ALL, ipId1));
//		zkServer.createZKNode("/aclthpffcj/iptest", "iptest".getBytes(), aclsIP);

        // 验证ip是否有权限
        zkServer.getZookeeper().setData("/aclthpffcj/iptest", "now".getBytes(), 0);
        Stat stat = new Stat();
        byte[] data = zkServer.getZookeeper().getData("/aclthpffcj/iptest", false, stat);
        System.out.println(new String(data));
        System.out.println(stat.getVersion());
    }

    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(ZooKeeper zookeeper) {
        this.zookeeper = zookeeper;
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
