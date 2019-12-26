package cn.edu.nju.starter.utils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;

/**
 * Created by thpffcj on 2019/12/25.
 */
public class AclUtils {

    public static String getDigestUserPwd(String id) throws Exception {
        return DigestAuthenticationProvider.generateDigest(id);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, Exception {
        String id = "thpffcj:123456";
        String idDigested = getDigestUserPwd(id);
        System.out.println(idDigested);
    }
}
