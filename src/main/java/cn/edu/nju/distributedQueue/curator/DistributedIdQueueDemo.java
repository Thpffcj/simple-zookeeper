package cn.edu.nju.distributedQueue.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedIdQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

/**
 * Created by thpffcj on 2020/1/5.
 *
 * 带Id的分布式队列：DistributedIdQueue
 */
public class DistributedIdQueueDemo {

    private static final String PATH = "/example/queue";

    public static void main(String[] args) throws Exception {

        TestingServer server = new TestingServer();

        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(
                    server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.getCuratorListenable().addListener(
                    (client1, event) -> System.out.println("CuratorEvent：" + event.getType().name()));
            client.start();

            // DistributedIdQueue和上面的队列类似，但是可以为队列中的每一个元素设置一个ID。 可以通过ID把队列中任意的元素移除
            DistributedIdQueue<String> queue;
            QueueConsumer<String> consumer = createQueueConsumer();
            QueueBuilder<String> builder = QueueBuilder.builder(
                    client, consumer, createQueueSerializer(), PATH);
            queue = builder.buildIdQueue();
            queue.start();

            for (int i = 0; i < 10; i++) {
                queue.put(" test-" + i, "Id" + i);
                Thread.sleep((long) (15 * Math.random()));
                queue.remove("Id" + i);
            }

            Thread.sleep(10000);

            queue.close();
            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }

    private static QueueSerializer<String> createQueueSerializer() {
        return new QueueSerializer<String>() {
            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }
        };
    }

    private static QueueConsumer<String> createQueueConsumer() {

        return new QueueConsumer<String>() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("connection new state: " + newState.name());
            }

            @Override
            public void consumeMessage(String message) throws Exception {
                System.out.println("consume one message: " + message);
            }
        };
    }
}