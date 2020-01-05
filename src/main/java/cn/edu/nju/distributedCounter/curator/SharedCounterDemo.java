package cn.edu.nju.distributedCounter.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Created by thpffcj on 2020/1/4.
 *
 * SharedCount代表计数器， 可以为它增加一个SharedCountListener，当计数器改变时此Listener可以监听到改变的事件，而
 * SharedCountReader可以读取到最新的值， 包括字面值和带版本信息的值VersionedValue。SharedCount必须调用start()方法开启，
 * 使用完之后必须调用close关闭它
 */
public class SharedCounterDemo implements SharedCountListener {

    private static final int QTY = 5;
    private static final String PATH = "/examples/counter";

    /**
     * 在这个例子中，我们使用baseCount来监听计数值(addListener方法来添加SharedCountListener )。 任意的SharedCount，
     * 只要使用相同的path，都可以得到这个计数值。 然后我们使用5个线程为计数值增加一个10以内的随机数。相同的path的SharedCount
     * 对计数值进行更改，将会回调给baseCount的SharedCountListener
     */
    public static void main(String[] args) throws Exception {

        final Random rand = new Random();
        SharedCounterDemo example = new SharedCounterDemo();
        TestingServer server = new TestingServer();

        // 重试策略，初试时间1秒，重试3次
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        // 通过工厂创建Curator
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(policy).build();
        // 开启连接
        client.start();

        SharedCount baseCount = new SharedCount(client, PATH, 0);
        baseCount.addListener(example);
        baseCount.start();

        List<SharedCount> examples = Lists.newArrayList();
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; i++) {
            final SharedCount count = new SharedCount(client, PATH, 0);
            examples.add(count);

            Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    count.start();
                    Thread.sleep(rand.nextInt(10000));
                    int add = count.getCount() + rand.nextInt(10);
                    System.out.println("要更改的值为：" + add);
                    // 这里我们使用trySetCount去设置计数器
                    // 第一个参数提供当前的VersionedValue，如果期间其它client更新了此计数值，你的更新可能不成功
                    // setCount是强制更新计数器的值
                    boolean b = count.trySetCount(count.getVersionedValue(), add);
                    System.out.println("更改结果为：" + b);
                    return null;
                }
            };
            service.submit(task);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);

        for (int i = 0; i < QTY; ++i) {
            examples.get(i).close();
        }
        baseCount.close();

        CloseableUtils.closeQuietly(server);
    }

    @Override
    public void countHasChanged(SharedCountReader sharedCountReader, int newCount) throws Exception {
        System.out.println("Counter's value is changed to " + newCount);
    }

    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        System.out.println("State changed: " + connectionState.toString());
    }
}
