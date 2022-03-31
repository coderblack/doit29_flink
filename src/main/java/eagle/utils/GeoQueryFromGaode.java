package eagle.utils;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class GeoQueryFromGaode {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("gps01")
                .setTopics("unknown-gps")
                .build();

        // 读kafka中的 未知地理位置的  gps坐标数据
        DataStreamSource<String> gpsSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "gpsSource");

        // 请求高德地图服务，得到地理位置信息
        gpsSource.process(new ProcessFunction<String, String>() {
            BloomFilter<CharSequence> bloomFilter;
            Table table;
            CloseableHttpClient httpClient;


            @Override
            public void open(Configuration parameters) throws Exception {

                // 构造一个布隆过滤器
                bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 10000000, 0.01);

                // 构造一个hbase连接
                Connection conn = HbaseConnUtil.getConn();
                table = conn.getTable(TableName.valueOf("eagle_geo_dict"));

                // 构造一个http的客户端对象
                httpClient = HttpClients.createDefault();

            }



            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {

                // s: "160.65004531926763,70.756984446036"  ==> 经度，纬度

                if(!bloomFilter.mightContain(s)) {
                    // 构造一个请求
                    HttpGet httpGet = new HttpGet(String.format("https://restapi.amap.com/v3/geocode/regeo?key=e384b6b8bc2f8e9e9e92a9cf969da45c&location=%s&output=JSON", s));

                    // 发送请求
                    CloseableHttpResponse response = httpClient.execute(httpGet);

                    // 获取响应
                    String result = EntityUtils.toString(response.getEntity());

                    // 解析数据
                    JSONObject jsonObject = JSON.parseObject(result);

                    JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                    JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                    String province = addressComponent.getString("province");
                    String city = addressComponent.getString("city");
                    String district = addressComponent.getString("district");

                    // 成功请求到高德后，把本条坐标数据，放入布隆过滤器
                    bloomFilter.put(s);

                    System.out.println(String.format("请求高德成功： %s,%s,%s  ",province,city,district));


                    // 将得到的结果，写入hbase中我们自己的地理位置参考维表
                    String[] gps = s.split(",");
                    String geohashCode = GeoHash.geoHashStringWithCharacterPrecision(Double.parseDouble(gps[1]), Double.parseDouble(gps[0]), 5);
                    Put put = new Put(Bytes.toBytes(geohashCode));
                    put.addColumn("f".getBytes(), "p".getBytes(), Bytes.toBytes(province));
                    put.addColumn("f".getBytes(), "c".getBytes(), Bytes.toBytes(city));
                    put.addColumn("f".getBytes(), "r".getBytes(), Bytes.toBytes(district));
                    table.put(put);
                }else{
                    System.out.println(String.format("本次坐标： %s , 之前已经请求过，直接跳过 ",s));
                }
            }
        });


        env.execute();
    }
}
