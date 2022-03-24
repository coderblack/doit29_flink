package cn.doitedu.eagle;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE mysql_binlog (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " age INT ,\n" +
                " primary key(id) not enforced" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'abc',\n" +
                " 'server-time-zone' = 'Asia/Shanghai' ,   \n" +
                " 'table-name' = 'flink_test'\n" +
                ")");

        tenv.executeSql("CREATE TEMPORARY TABLE flink_test2 (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " age INT ,\n" +
                " primary key(id) not enforced \n" +
                ") WITH (   \n" +
                " 'connector' = 'jdbc',    \n" +
                " 'url' = 'jdbc:mysql://localhost:3306/abc?serverTimezone=Asia/Shanghai', \n" +
                " 'username' = 'root',   \n" +
                " 'password' = '123456',    \n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver' ,\n" +
                " 'table-name' = 'flink_test2'    \n" +
                ")");

        tenv.executeSql("insert into flink_test2 select id,name,age from mysql_binlog").print();

    }
}
