package cn.doitedu.flink.demos;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/6
 **/
public class ConnectorDemos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStateBackend(new HashMapStateBackend());
        DataStreamSource<String> source = env.fromElements("1,aa,38", "2,bb,30", "3,cc,36");

        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> op1 = source.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        }).returns(TypeInformation.of(new TypeHint<Tuple3<Integer, String, Integer>>() {
        }));

        SinkFunction<Tuple3<Integer, String, Integer>> sink1 = JdbcSink.sink(
                "insert into abc.flink_test values (?,?,?) on duplicate key update name=? ,age = ?",
                new JdbcStatementBuilder<Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<Integer, String, Integer> data) throws SQLException {
                        preparedStatement.setInt(1, data.f0);
                        preparedStatement.setString(2, data.f1);
                        preparedStatement.setInt(3, data.f2);
                        preparedStatement.setString(4, data.f1);
                        preparedStatement.setInt(5, data.f2);
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://localhost:3306")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        SinkFunction<Tuple3<Integer, String, Integer>> sink2 = JdbcSink.exactlyOnceSink(
                "insert into flink_test values (?,?,?) on duplicate key update name=? ,age = ?",
                new JdbcStatementBuilder<Tuple3<Integer, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<Integer, String, Integer> data) throws SQLException {
                        preparedStatement.setInt(1, data.f0);
                        preparedStatement.setString(2, data.f1);
                        preparedStatement.setInt(3, data.f2);
                        preparedStatement.setString(4, data.f1);
                        preparedStatement.setInt(5, data.f2);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(2)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                .withTransactionPerConnection(true)
                .build(),
                () -> {
                    MysqlXADataSource xaDataSource = new com.mysql.cj.jdbc.MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://localhost:3308/");
                    xaDataSource.setUser("root");
                    xaDataSource.setPassword("123456");
                    xaDataSource.setDatabaseName("abc");
                    return xaDataSource;
                });


        KafkaSink sink3 = KafkaSink.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder().setTopic("abc").setValueSerializationSchema(new SimpleStringSchema()).build())
                .setTransactionalIdPrefix("tx-flink")
                .build();


        /*source.sinkTo(sink3);*/
        source.map(s-> Arrays.asList(s.split(","))).returns(new TypeHint<List<String>>() {
        }).print();
        env.execute();
    }
}
