package cn.edu.nju.kafka.consumer.method1;

import cn.edu.nju.kafka.producer.StockQuotationInfo;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by thpffcj on 2020/3/30.
 */
public class ESUtil {

    private static TransportClient client = null;

    public static void initClient() {
        try {
            // on startup
            Settings settings = Settings.builder().put("cluster.name", "*-*.com-es").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("10.45.*.*"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void closeClient() {
        client.close();
    }

    public static void putToEs(String record) {

        try {
            String id = "9";
            String msgType = "";
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

            StockQuotationInfo info = JSON.parseObject(record, StockQuotationInfo.class);
            String timestamp = String.valueOf(info.getTradeTime());

            // ES写数据
            BulkRequestBuilder bulkRequest = client.prepareBulk();

            IndexRequestBuilder request1 = client.prepareIndex("kafka_to_es_test", "kafkadata",
                    String.valueOf(id)).setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("time_stamp", sdf.parse(timestamp))
                    .field("msg_type", msgType)
                    .endObject()
            );
            IndexRequestBuilder request2 = client.prepareIndex("kafka_to_es_test", "kafkadata",
                    String.valueOf(id)).setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("time_stamp", sdf.parse(timestamp))
                    .field("msg_type", msgType)
                    .endObject()
            );

            bulkRequest.add(request1);
            bulkRequest.add(request2);

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkRequest.toString());
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
