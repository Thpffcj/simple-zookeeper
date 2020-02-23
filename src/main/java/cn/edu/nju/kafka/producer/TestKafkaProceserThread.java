package cn.edu.nju.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by thpffcj on 2020/2/22.
 *
 * Kafka producer是单线程的，但是有时候会有单进程大量写入数据到kafka的需求，这时单线程的producer往往就难以满足需求，但是如果在
 * 每个发送线程里new一个新的producer又太浪费资源，因此我们想是否能有办法像数据库连接池一样复用kafka连接最终实现多线程写入
 */
public class TestKafkaProceserThread {

    // Kafka配置文件
    public static final String TOPIC_NAME = "test";
    private static final String BROKER_LIST = "192.168.199.128:9092,192.168.199.129:9092,192.168.199.130:9092";
    // 实例池大小
    public static final int producerNum = 50;

    // 阻塞队列实现生产者实例池,获取连接作出队操作，归还连接作入队操作
    public static BlockingQueue<KafkaProducer<String, String>> queue = new LinkedBlockingQueue<>(producerNum);

    // 初始化producer实例池
    static {
        for (int i = 0; i <producerNum ; i++) {
            Properties properties = intiConfig();
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
            queue.add(kafkaProducer);
        }
    }

    private static Properties intiConfig() {
        Properties properties = new Properties();
        // kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // key的序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value的序列化类
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    // 生产者发送线程
    static class SendThread extends Thread {
        String msg;
        public SendThread(String msg){
            this.msg=msg;
        }
        public void run(){
            ProducerRecord record = new ProducerRecord(TOPIC_NAME, msg);
            try {
                KafkaProducer<String, String> kafkaProducer = queue.take(); // 从实例池获取连接,没有空闲连接则阻塞等待
                kafkaProducer.send(record);
                queue.put(kafkaProducer); // 归还kafka连接到连接池队列
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // 测试
    public static void main(String[]args){
        for (int i = 0; i < 100 ; i++) {
            SendThread sendThread=new SendThread("test multi-thread producer!");
            sendThread.start();
        }
    }
}
