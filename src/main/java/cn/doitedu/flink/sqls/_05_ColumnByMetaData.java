package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

public class _05_ColumnByMetaData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        /**
         * 1. 用建表语句，把底层connector中的metadata声明为逻辑字段举例
         */
         tenv.executeSql(SqlHolder.getSql(11));
         tenv.executeSql("select * from action_event").print();


        /**
         * 2. 从dataStream 注册 视图时，如何在表结构中声明 metadata为逻辑字段 举例
         */
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<UserAction> stream = source.map(new MapFunction<String, UserAction>() {
                    @Override
                    public UserAction map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserAction(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], Long.parseLong(split[3]),split[4]);
                    }
                })
                // 为流分配event-time，以及指定watermark生成策略
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserAction>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserAction>() {
                            @Override
                            public long extractTimestamp(UserAction element, long recordTimestamp) {
                                return element.getActionTime();
                            }
                        }));

        // 把 流 注册 sql视图时，通过元数据"rowtime"引用流中的 event-time，声明成sql表的一个逻辑字段 rt
        tenv.createTemporaryView("t_action", stream, Schema.newBuilder()
                .columnByMetadata("rt", DataTypes.TIMESTAMP(3), "rowtime")   // rowtime 代表 流中时间（事件时间）
                .build());

        tenv.executeSql("select * from t_action").print();

        env.execute();

    }
}
