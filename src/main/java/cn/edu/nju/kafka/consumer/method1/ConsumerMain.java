package cn.edu.nju.kafka.consumer.method1;

/**
 * Created by thpffcj on 2020/2/22.
 */
public class ConsumerMain {

    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}
