package cn.doitedu.flink.apis;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ApiExercise_14_KafKaSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new HashMapStateBackend());

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        // 读数据，写入kafka
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic-name")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("doitedu-")
                .build();


        source.sinkTo(kafkaSink);

        env.execute();


    }
}
