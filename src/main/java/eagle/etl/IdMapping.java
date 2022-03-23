package eagle.etl;

import eagle.functions.IdMappingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class IdMapping {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取kafka中的用户行为日志数据流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("eagle-001")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("eagle-applog")
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");

        // 真实数据：{"account":"gesw,azt","appid":"cn.doitedu.study.Yiee","appversion":"8.3","carrier":"360移动","deviceid":"CGDNHCPWUTQQ","devicetype":"IPHONE-8","eventid":"launch","ip":"155.74.103.111","latitude":34.756984446036,"longitude":113.65004531926762,"nettype":"WIFI","osname":"ios","osversion":"8.8","properties":{},"releasechannel":"酷市场-CoolMar","resolution":"1024*768","sessionid":"vtlmbxdo","timestamp":1645968611380}

        // 测试数据： d01,zhangHao
        SingleOutputStreamOperator<Tuple2<String, String>> stream2 = sourceStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });

        // 去做idMapping
        stream2.keyBy(tp -> tp.f0)
                .process(new IdMappingFunction());


    }
}
