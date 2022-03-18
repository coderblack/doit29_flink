package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * 带事件时间属性，及有WaterMark推进的表
 */
public class _05_TimeAttributeAndWaterMarkTable {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 将 kafka中的一个topic映射成一个视图（表）
        tenv.executeSql(SqlHolder.getSql(10));
        tenv.executeSql("select guid,action_time,event_id,action_timelong,channel,action_time_ltz, current_watermark(action_time_ltz)  from action_event").print();

    }
}
