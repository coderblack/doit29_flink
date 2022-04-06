package eagle.etl;

import com.alibaba.fastjson.JSON;
import eagle.functions.TrafficAnalyseFunc;
import eagle.pojo.EventBean;
import eagle.pojo.TrafficBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.awt.*;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/6
 **/
public class ApplogTrafficAnalyseV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 构造一个kafka的source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("dwd-applog-detail")
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // 从kafka的dwd明细topic读取数据
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "dwd-applog");

        // 带上事件时间语义和watermark生成策略的  bean对象数据流
        SingleOutputStreamOperator<EventBean> beanStream = sourceStream.map(json -> JSON.parseObject(json, EventBean.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        SingleOutputStreamOperator<TrafficBean> trafficStream = beanStream.keyBy(bean -> bean.getGuid())
                .process(new TrafficAnalyseFunc());


        trafficStream.print();

        env.execute();
    }
}
