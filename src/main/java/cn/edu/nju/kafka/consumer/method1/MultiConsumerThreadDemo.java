package cn.edu.nju.kafka.consumer.method1;

import cn.edu.nju.kafka.producer.StockQuotationInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * Created by thpffcj on 2020/2/23.
 */
public class MultiConsumerThreadDemo {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    static int count = 0;

    public static Properties initConfig(){
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        int consumerThreadNum = 4;
        for(int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(props, topic).start();
        }
    }

    /**
     * 内部类 KafkaConsumerThread 代表消费线程，其内部包裹着一个独立的 KafkaConsumer 实例。通过外部类的 main() 方法来启动多
     * 个消费线程，消费线程的数量由 consumerThreadNum 变量指定。一般一个主题的分区数事先可以知晓，可以将 consumerThreadNum
     * 设置成不大于分区数的值，如果不知道主题的分区数，那么也可以通过 KafkaConsumer 类的 partitionsFor() 方法来间接获取，
     * 进而再设置合理的 consumerThreadNum 值。
     *
     * 上面这种多线程的实现方式和开启多个消费进程的方式没有本质上的区别，它的优点是每个线程可以按顺序消费各个分区中的消息。
     * 缺点也很明显，每个消费线程都要维护一个独立的TCP连接，如果分区数和 consumerThreadNum 的值都很大，那么会造成不小的系统开销。
     *
     * 如果对消息的处理非常迅速，那么 poll() 拉取的频次也会更高，进而整体消费的性能也会提升；相反，如果在这里对消息的处理缓慢，比
     * 如进行一个事务性操作，或者等待一个RPC的同步响应，那么 poll() 拉取的频次也会随之下降，进而造成整体消费性能的下降。一般而言，
     * poll() 拉取消息的速度是相当快的，而整体消费的瓶颈也正是在处理消息这一块，如果我们通过一定的方式来改进这一部分，那么我们就能
     * 带动整体消费性能的提升。
     */
    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties props, String topic) {
            this.kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                // 该方法会在消费者停止读取消息之后，再均衡开始之前就调用
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("再均衡即将触发");
                    // 提交已经处理的偏移量
                    kafkaConsumer.commitSync(offsets);
                }

                // 该方法会在重新分配分区之后，消费者开始读取消息之前被调用
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                }
            });
        }

        @Override
        public void run(){
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        // 处理消息模块    ①
                        System.out.println(record);
                        JSON.parseObject(record.value(), StockQuotationInfo.class);
                        ESUtil.putToEs(record.value());

                        // 记录每个主题的每个分区的偏移量
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata offsetAndMetadata =
                                new OffsetAndMetadata(record.offset() + 1, "no metaData");
                        offsets.put(topicPartition, offsetAndMetadata);
                        // TopicPartition 重写过 hashCode 和 equals 方法，所以能够保证同一主题和分区的实例不会被重复添加
                        if (count % 100 == 0) {
                            // 提交特定偏移量
                            kafkaConsumer.commitAsync(offsets, new OffsetCommitCallback() {
                                @Override
                                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                    if (exception != null) {
                                        System.out.println("错误处理");
                                        offsets.forEach((x, y) -> System.out.printf("topic = %s,partition = %d, offset = %s \n",
                                                x.topic(), x.partition(), y.offset()));
                                    }
                                }
                            });
                        }
                        count++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.commitSync();
                kafkaConsumer.close();
            }
        }
    }
}
