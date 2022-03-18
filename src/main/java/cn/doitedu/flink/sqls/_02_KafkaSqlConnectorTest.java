package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

public class _02_KafkaSqlConnectorTest {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 把外部数据源（kafka），利用connector，注册成一个视图（表）
        tenv.executeSql(SqlHolder.getSql(1));  // 执行该建表语句后，系统的元数据中就存在了这张表

        // sql 查询
        // 筛选出action_timelong>10的数据
        tenv.executeSql("select * from action_event where action_timelong>10").print();

    }
}
