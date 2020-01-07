package cn.edu.nju.distributedQueue.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
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
 * 优先级队列对队列中的元素按照优先级进行排序。Priority越小，元素越靠前，越先被消费掉
 */
public class DistributedPriorityQueueDemo {

    private static final String PATH = "/example/queue";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer();

        CuratorFramework client = null;
        DistributedPriorityQueue<String> queue = null;

        try {
            client = CuratorFrameworkFactory.newClient(
                    server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.getCuratorListenable().addListener(
                    (client1, event) -> System.out.println("CuratorEvent: " + event.getType().name()));
            client.start();

            QueueConsumer<String> consumer = createQueueConsumer();
            QueueBuilder<String> builder = QueueBuilder.builder(
                    client, consumer, createQueueSerializer(), PATH);
            queue = builder.buildPriorityQueue(0);
            queue.start();

            for (int i = 0; i < 10; i++) {
                int priority = (int) (Math.random() * 100);
                System.out.println("test-" + i + " priority:" + priority);
                // 当优先级队列得到元素增删消息时，它会暂停处理当前的元素队列，然后刷新队列
                // 有时候你可能会有错觉，优先级设置并没有起效。那是因为优先级是对于队列积压的元素而言，如果消费速度过快有可能
                // 出现在后一个元素入队操作之前前一个元素已经被消费，这种情况下DistributedPriorityQueue会退化
                // 为DistributedQueue
                queue.put("test-" + i, priority);
                Thread.sleep((long) (50 * Math.random()));
            }

            Thread.sleep(10000);

        } catch (Exception ex) {

        } finally {
            CloseableUtils.closeQuietly(queue);
            CloseableUtils.closeQuietly(client);
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
                Thread.sleep(1000);
                System.out.println("consume one message: " + message);
            }
        };
    }
}
