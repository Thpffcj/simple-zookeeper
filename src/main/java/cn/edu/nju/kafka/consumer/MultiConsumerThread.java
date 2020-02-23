package cn.edu.nju.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by thpffcj on 2020/2/22.
 *
 * 将处理消息模块改成多线程的实现方式
 */
public class MultiConsumerThread {

    public static final String BROKER_LIST = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                props, topic, Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    /**
     * KafkaConsumerThread 类对应的是一个消费线程，里面通过线程池的方式来调用 RecordHandler 处理一批批的消息。
     * 注意 KafkaConsumerThread 类中 ThreadPoolExecutor 里的最后一个参数设置的是 CallerRunsPolicy()，这样可以防止线程池
     * 的总体消费能力跟不上 poll() 拉取的能力，从而导致异常现象的发生。
     * 第三种实现方式还可以横向扩展，通过开启多个 KafkaConsumerThread 实例来进一步提升整体的消费能力。
     */
    public static class KafkaConsumerThread extends Thread {

        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;

        public KafkaConsumerThread(Properties props, String topic, int threadNumber) {
            kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNumber = threadNumber;
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber,
                    0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    // RecordHandler 类是用来处理消息的
    public static class RecordsHandler extends Thread {
        public final ConsumerRecords<String, String> records;

        public RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run(){
            //处理records.

            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
                //处理tpRecords.
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
//                synchronized (offsets) {
//                    if (!offsets.containsKey(tp)) {
//                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
//                    }else {
//                        long position = offsets.get(tp).offset();
//                        if (position < lastConsumedOffset + 1) {
//                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
//                        }
//                    }
//                }
            }
        }
    }

    private static Properties initConfig() {
        Properties properties = new Properties();
        // kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // key的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value的序列化类
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
