package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestLocalKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("[::1]:9092")
                .setTopics("abc")
                .setGroupId("g1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<String> kfk = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");
        kfk.print();

        env.execute();

    }

}
