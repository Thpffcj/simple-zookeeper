package cn.edu.nju.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Created by thpffcj on 2020/2/22.
 *
 * 若在ProducerRecord中不指定哪个分区和分区键，则消息按照轮询算法将消息均衡的分布到各个分区中。若指定了分区键，kafka会使用散列
 * 算法计算key,从而找到对应的分区，所以只要两条消息分区键一致则会发送到同一个分区。但是一旦增加或者减少分区后，散列的结果不同就没
 * 法保证同一个key还能发送到上次对应的分区。
 *
 * 自定义的分区策略
 * 当分区键是xuzy时，都将其分配到最后一个分区中
 * 如何使用
 * kafkaProps.put("partitioner.class", "cn.edu.nju.kafka.producer.MyPartitioner");
 */
public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 根据主题名称获取到分区
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartition = partitionInfos.size();
        if( (key == null) || !(key instanceof String)){
            throw new InvalidRecordException("key is error");
        }
        if("xuzy".equals(key)){
            return numPartition;
        }
        return (Math.abs(Utils.murmur2(keyBytes))) % (numPartition - 1);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
