package cn.doitedu.flink.exercise;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import pojos.Student;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * 从kafka读取 a,1 a,2 a,3 a,4 ... a,200
 *            b,1 b,2 b,3 b,4 ... b,200
 * 然后,对数据按字母keyBy
 * 然后，对整数做映射： (x,n) -> (x,10*n)
 * 然后, 将数据写入mysql
 *
 * 把容错相关配置打满！！
 * 在上述的一些算子中，安排一些 随机异常抛出（整个200条数据中大概抛出2次异常）
 *
 * 然后，观察你的mysql中的结果，是否正确（是否有重复或者有丢失）
 *
 */
public class 端到端精确一致性的测试 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "file:///D:/checkpoint/7ecbd4f9cad68d43106957c42109bcde/chk-544");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE,1000));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setTopics("b05")
                .setGroupId("x06")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
                // kafkaSource的做状态checkpoint时，默认会向__consumer_offsets提交一下状态中记录的偏移量
                // 但是，flink的容错并不优选依赖__consumer_offsets中的记录，所以可以关闭该默认机制
                .setProperty("commit.offsets.on.checkpoint","false")  // 默认是true

                // kafkaSource启动时，从哪里去获取起始唯一的策略设置，如果是 committedOffsets ，则是从之前所记录的便宜量开始
                // 如果没有可用的之前记录的偏移量, 则用策略 OffsetResetStrategy.LATEST 来决定
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();

       /* SinkFunction<String> jdbcSink = JdbcSink.sink(
                "insert into t_once values (?)",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doit01:3306/abc")
                        .withUsername("root")
                        .withPassword("ABC123.abc123")
                        .build()
        );*/

        SinkFunction<String> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into t_once values (?)",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1, s);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个未完成的事务，必须把该参数设置为true
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource就是jdbc连接，不过它是支持分布式事务的连接
                        // 而且它的构造方法，不同的数据库构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://doit01:3306/abc");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("ABC123.abc123");
                        return xaDataSource;
                    }
                }
        );



        DataStreamSource<String> s1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");

        /*s1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.equals("x") && RandomUtils.nextInt(1,5)%3==0) throw new Exception("抛了个异常......");

                return value;
            }
        }).print();*/


        s1.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if (s.equals("x") && 3 / RandomUtils.nextInt(0, 3) == 1) ;
                return s;
            }
        });

        s1.addSink(exactlyOnceSink);

        env.execute();
    }
}
