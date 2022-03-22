package cn.doitedu.eagle;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class _03_DataEtl {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doit01:8020/ckpt");
        KafkaSource<String> kfkSource = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("applog")
                .build();



    }
}
