package cn.doitedu.flink.apis;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import cn.doitedu.flink.pojos.Student;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ApiExercise_15_JdbcSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new HashMapStateBackend());

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        // 读数据，写入 mysql
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        SingleOutputStreamOperator<Student> studentStream = source.map(s -> {
            String[] arr = s.split(",");
            return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
        }).returns(Student.class);


        // 构造一个 jdbc 的sink
        // 该sink的底层并没有去开启mysql的事务，所以并不能真正保证 端到端的 精确一次
        SinkFunction<Student> jdbcSink = JdbcSink.sink(
                "insert into flink_stu values (?,?,?,?) on duplicate key update name=?,gender=?,score=? ",
                new JdbcStatementBuilder<Student>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Student student) throws SQLException {
                        preparedStatement.setInt(1, student.getId());
                        preparedStatement.setString(2, student.getName());
                        preparedStatement.setString(3, student.getGender());
                        preparedStatement.setFloat(4, (float) student.getScore());

                        preparedStatement.setString(5,student.getName());
                        preparedStatement.setString(6, student.getGender());
                        preparedStatement.setFloat(7, (float) student.getScore());
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
        );


        SinkFunction<Student> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into flink_stu values (?,?,?,?) on duplicate key update name=?,gender=?,score=? ",
                /*"insert into flink_stu values (?,?,?,?)  ",*/
                (PreparedStatement preparedStatement, Student student) -> {
                    preparedStatement.setInt(1, student.getId());
                    preparedStatement.setString(2, student.getName());
                    preparedStatement.setString(3, student.getGender());
                    preparedStatement.setFloat(4, (float) student.getScore());

                    preparedStatement.setString(5, student.getName());
                    preparedStatement.setString(6, student.getGender());
                    preparedStatement.setFloat(7, (float) student.getScore());
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true) // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
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


        // 把构造好的sink添加到流中
        studentStream.addSink(exactlyOnceSink);

        env.execute();
    }
}
