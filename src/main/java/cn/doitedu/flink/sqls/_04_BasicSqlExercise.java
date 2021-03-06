package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * sql编程模式下的基础sql查询练习
 */
public class _04_BasicSqlExercise {

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 将kafka中的一个topic映射成一个视图（表）
        tenv.executeSql(SqlHolder.getSql(2));

        // 统计如下需求：
        // 1.过滤出所有 action_timelong >50的行为事件数据,并将结果中的 action_timelong 返回为毫秒数
        tenv.executeSql(SqlHolder.getSql(3))/*.print()*/;

        // 访客数
        tenv.executeSql(SqlHolder.getSql(4))/*.print()*/;

        // 各渠道的访客总数
        tenv.executeSql(SqlHolder.getSql(5))/*.print()*/;

        // 各事件的发生人数
        tenv.executeSql(SqlHolder.getSql(6))/*.print()*/;

        // 各事件的行为总时长
        tenv.executeSql(SqlHolder.getSql(7))/*.print()*/;

        // 各渠道的行为事件种类数
        tenv.executeSql(SqlHolder.getSql(8))/*.print()*/;

        // 各渠道中，行为总时长最大的前5个人及其行为总时长
        tenv.executeSql(SqlHolder.getSql(9)).print();


    }
}
