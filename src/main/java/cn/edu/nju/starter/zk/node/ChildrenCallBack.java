package cn.edu.nju.starter.zk.node;

import org.apache.zookeeper.AsyncCallback;

import java.util.List;

/**
 * Created by thpffcj on 2019/12/27.
 */
public class ChildrenCallBack implements AsyncCallback.ChildrenCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        for (String s : children) {
            System.out.println(s);
        }
        System.out.println("ChildrenCallback:" + path);
        System.out.println((String) ctx);
    }
}
