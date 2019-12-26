package cn.edu.nju.starter.zk.node;

import org.apache.zookeeper.AsyncCallback;

/**
 * Created by thpffcj on 2019/12/24.
 */
public class DeleteCallBack implements AsyncCallback.VoidCallback {

    @Override
    public void processResult(int rc, String path, Object ctx) {
        System.out.println("删除节点" + path);
        System.out.println((String) ctx);
    }
}
