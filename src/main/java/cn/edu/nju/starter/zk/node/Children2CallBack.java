package cn.edu.nju.starter.zk.node;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Created by thpffcj on 2019/12/26.
 */
public class Children2CallBack implements AsyncCallback.Children2Callback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        for (String s : children) {
            System.out.println(s);
        }
        System.out.println("ChildrenCallback:" + path);
        System.out.println((String) ctx);
        System.out.println(stat.toString());
    }
}
