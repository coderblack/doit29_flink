package cn.doitedu.flink.demos;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcExactlyOnceSinkTest {

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        /*conf.setString("execution.savepoint.path","file:///d:/ckpt/a9c4e1622aea74b9bffb7f37dd51a311/chk-104");*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.noRestart());


        //DataStreamSource<String> ds = env.socketTextStream("localhost", 9099);
        DataStreamSource<String> ds = env.addSource(new DataGen());

        KafkaRecordSerializationSchema<String> abc = new KafkaRecordSerializationSchemaBuilder<String>()
                .setTopic("abc")
                .setValueSerializationSchema(new SimpleStringSchema()).build();

        KafkaSink<String> kfkSink = KafkaSink.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setRecordSerializer(abc)
                .build();

        SinkFunction<String> mysqlSink = JdbcSink.<String>exactlyOnceSink(
                "insert into tt values(?,?) on duplicate key update id=? ,name=? ",
                (preparedStatement, s) -> {
                    String[] arr = s.split(",");
                    preparedStatement.setInt(1, Integer.parseInt(arr[0]));
                    preparedStatement.setString(2, arr[1]);
                    preparedStatement.setInt(3, Integer.parseInt(arr[0]));
                    preparedStatement.setString(4, arr[1]);
                },
                JdbcExecutionOptions.defaults(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true)
                        .withRecoveredAndRollback(true).build(),
                () -> {
                    MysqlXADataSource xaDataSource = new MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://doit01:3306/abc");
                    xaDataSource.setUser("root");
                    xaDataSource.setPassword("ABC123.abc123");
                    return xaDataSource;
                }
        );
        // ds.sinkTo(kfkSink);
        ds.addSink(mysqlSink);

        env.execute();


    }

}

class DataGen implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (true) {
            ctx.collect(RandomUtils.nextInt(1, 10) + "," + RandomStringUtils.random(1, "abcde"));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}

