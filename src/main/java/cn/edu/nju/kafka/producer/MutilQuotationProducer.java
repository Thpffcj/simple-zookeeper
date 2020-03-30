package cn.edu.nju.kafka.producer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by thpffcj on 2020/2/22.
 *
 * 1.创建一个ProducerRecord对象，里面包含要发送的主题，要发送的内容，还可以指定分区键或分区
 * 2.通过序列化器将键值和值对象序列化成字节数组使其能在网络上传输
 * 3.序列化后将数据传给分区器，若前面已经指定了分区就不做任何事直接将数据写入对应分区，若没有则根据键来选择一个分区写入。紧接着这条
 * 记录会被添加到一个批次中，这个批次的记录都会发往同一个主题和分区上。会有一个单独的进程将这些批次发送到broker上
 * 4.发送成功后返回RecordMetaData对象，包含主题和分区信息和分区偏移量，如果失败则尝试重发，几次失败后则返回错误信息
 *
 * 发送消息主要有三种方式:
 * a. 发送并忘记:发送后不关心是否正常到达，大多数会正常到达，kafka也会有重发机制,不过这种方式可能导致有时候一小部分消息丢失
 * b. 同步发送:发送完后返回一个Future对象，调用get()方法进行等待就知道是否成功(KafkaProducer.java的send(ProducerRecord<K, V> record)方法)
 * c. 异步发送:发送完并指定一个回调函数，服务器在返回响应时调用此函数(KafkaProducer.java的(ProducerRecord<K, V> record, Callback callback)方法)
 *
 */
public class MutilQuotationProducer {

    // topic name
    private static final String TOPIC = "stock-quotation";
    private static final String BROKER_LIST = "192.168.199.128:9092,192.168.199.129:9092,192.168.199.130:9092";
    private static KafkaProducer<String, String> kafkaProducer = null;

    static {
        Properties properties = initConfig();
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    /**
     * 初始化kafka配置
     */
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

    public static void main(String[] agrs) {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        ProducerRecord<String, String> record;
        try {
            for (int i = 0; i < 100; i++) {
                StockQuotationInfo stockQuotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(
                        TOPIC,
                        null,
                        stockQuotationInfo.getTradeTime(),
                        // key - value
                        stockQuotationInfo.getStockCode(), stockQuotationInfo.toString());
                executorService.submit(new ProducerThead(kafkaProducer, record));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
            executorService.shutdown();
        }
    }

    static class ProducerThead implements Runnable {

        private KafkaProducer<String, String> kafkaProducer;
        private ProducerRecord<String, String> producerRecord;

        public ProducerThead(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> producerRecord) {
            this.kafkaProducer = kafkaProducer;
            this.producerRecord = producerRecord;
        }

        @Override
        public void run() {
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null != e) {
                        System.out.println(e);
                    }
                    if (null != recordMetadata) {
                        System.out.println(String.format("offset:%s,partition:%s", recordMetadata.offset(), recordMetadata.partition()));
                    }
                }
            });
        }
    }

    /**
     * 产生消息
     */
    private static StockQuotationInfo createQuotationInfo() {
        StockQuotationInfo stockQuotationInfo = new StockQuotationInfo();
        Random r = new Random();
        Integer stockCode = 600100 + r.nextInt(10);
        float random = (float) Math.random();
        stockQuotationInfo.setCurrentPrice(random);
        stockQuotationInfo.setHighPrice(random + 1);
        stockQuotationInfo.setLowPrice(random - 1);
        stockQuotationInfo.setTradeTime(System.currentTimeMillis());
        stockQuotationInfo.setStockCode(stockCode.toString()); // 股票代码
        stockQuotationInfo.setStockName("股票_" + stockCode);
        return stockQuotationInfo;
    }

//    public static void main(String[] args) {
//        MutilQuotationProducer m = new MutilQuotationProducer();
//        System.out.println(createQuotationInfo());
//        System.out.println(JSON.parseObject(createQuotationInfo().toString(), StockQuotationInfo.class));
//    }
}
