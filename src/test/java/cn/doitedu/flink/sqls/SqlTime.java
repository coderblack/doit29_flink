package cn.doitedu.flink.sqls;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.Serializable;
import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.*;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/16
 *
 * 2,1,1647441091000
 * 2,2,1647441092000
 * 2,3,1647441093000
 * 2,4,1647441094000
 * 2,5,1647441095000
 * 2,6,1647441096000
 *
 **/
public class SqlTime {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Bean> s1 = source.map(new MapFunction<String, Bean>() {
            @Override
            public Bean map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Bean(arr[0], Integer.parseInt(arr[1]), Long.parseLong(arr[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean>() {
            @Override
            public long extractTimestamp(Bean bean, long recordTimestamp) {
                return bean.getTs();
            }
        }));

        /**
         * stream 转 table 时，声明事件时间属性及watermark
         *
         */
        // 声明watermark 方式1 ： 老版本方式
        /*Table table = tEnv.fromDataStream(s1,$("id"),$("score"),$("ts").rowtime());*/
        /* table.select($("id"),$("score"),$("ts"),currentWatermark($("ts"))).execute().print();*/

        // 声明watermark 方式2： 新版本方式
        /*Table table = tEnv.fromDataStream(s1, Schema.newBuilder()
         *//*.columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))*//* // metadata key 是写死的 rowtime （见ExternalDynamicSource） // 提取流连接器中的metadata-key： rowtime 数据到 列中
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3),"rowtime") // metadata key 是写死的 rowtime （见ExternalDynamicSource） // 提取流连接器中的metadata-key： rowtime 数据到 列中
                .watermark("rt", sourceWatermark())  // 声明为watermark属性 //  watermark("rowtime", "SOURCE_WATERMARK()")
                .build());
        // 获取当前watermark
        table.select($("id"),$("score"),$("ts"),currentWatermark($("rt"))).execute().print();*/

        // tableApi方式的 group window 聚合
        /*table.window(Tumble.over(lit(2).seconds()).on($("ts")).as("w"))
                .groupBy($("w"),$("id"))
                .select($("w").start(),$("w").end(),$("id"),$("score").sum())
                .execute()
                .print();*/

        /**
         * stream 转 view 时，声明事件时间属性及watermark
         * 测试已成立
         */

        // 声明时间属性及watermark
        tEnv.createTemporaryView("t", s1, Schema.newBuilder()
                .primaryKey("id")
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3)).watermark("rowtime", sourceWatermark())
                .build());
       /* tEnv.createTemporaryView("t",s1,$("id"),$("score"),$("rowtime").rowtime(),$("proc_time").proctime());*/

        // 获取当前watermark
        // tEnv.executeSql("select id,score,ts,rowtime,current_watermark(rowtime) from t").print();

        // 新版本方式： group  window 聚合
        /*tEnv.executeSql("SELECT window_start, window_end, SUM(score)\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE t, DESCRIPTOR(rowtime), INTERVAL '3' SECONDS))\n" +
                "  GROUP BY window_start, window_end").print();*/

        // 老版本方式： group  window 聚合
        /*tEnv.executeSql("SELECT TUMBLE_START(rowtime, INTERVAL '3' SECONDS), SUM(score)\n" +
                "FROM t \n" +
                "GROUP BY TUMBLE(rowtime, INTERVAL '3' SECONDS)").print();*/


        // 创建视图进行select转换，rowtime字段的watermark属性会继续传递
        /*tEnv.executeSql("create view t2 as select id,score,rowtime from t");
        tEnv.executeSql("select * from t2").print();
        tEnv.executeSql("SELECT window_start, window_end, SUM(score)\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE t2, DESCRIPTOR(rowtime), INTERVAL '3' SECONDS))\n" +
                "  GROUP BY window_start, window_end").print();*/

        // flink1.13版，对时区的支持和修正
        // 2022-03-17 15:33:19.802
        /*tEnv.executeSql("create view t3 as select timestamp '2022-03-17 15:33:19.802'  as ts1,TO_TIMESTAMP_LTZ(1647502399802,3)  as ts2");
        tEnv.executeSql("select * from t3").print();*/


        /**
         * group by  聚合
         */
        /*tEnv.executeSql(SqlHolder.getSql(1)).print()*/
        ;
        /* changelog类似的结果表
          +----+-----------+-------------+
          | op |        id |   sum_score |
          +----+-----------+-------------+
          | +I |         2 |           1 |
          | -U |         2 |           1 |
          | +U |         2 |           3 |
        */

        /**
         * windowing tvf
         */
        /*tEnv.executeSql(SqlHolder.getSql(2)).print();*/
        /*
           +----+-------------------------+-------------------------+-------------------------+-------------+
           | op |            window_start |              window_end |             window_time |   sum_score |
           +----+-------------------------+-------------------------+-------------------------+-------------+
           | +I | 2022-03-16 22:31:30.000 | 2022-03-16 22:31:32.000 | 2022-03-16 22:31:31.999 |           1 |
           | +I | 2022-03-16 22:31:32.000 | 2022-03-16 22:31:34.000 | 2022-03-16 22:31:33.999 |           5 |
           | +I | 2022-03-16 22:31:34.000 | 2022-03-16 22:31:36.000 | 2022-03-16 22:31:35.999 |           9 |
           | +I | 2022-03-16 22:31:36.000 | 2022-03-16 22:31:38.000 | 2022-03-16 22:31:37.999 |          12 |
        */

        /*tEnv.executeSql(SqlHolder.getSql(3)).print();*/
        /*
           +----+-------------------------+-------------------------+-----+-------------------------+-------------+
           | op |            window_start |              window_end |  id |             window_time |   sum_score |
           +----+-------------------------+-------------------------+-----+-------------------------+-------------+
           | +I | 2022-03-16 22:31:30.000 | 2022-03-16 22:31:32.000 |   2 | 2022-03-16 22:31:31.999 |           1 |
           | +I | 2022-03-16 22:31:30.000 | 2022-03-16 22:31:32.000 |   1 | 2022-03-16 22:31:31.999 |           1 |
           | +I | 2022-03-16 22:31:32.000 | 2022-03-16 22:31:34.000 |   2 | 2022-03-16 22:31:33.999 |           5 |
           | +I | 2022-03-16 22:31:32.000 | 2022-03-16 22:31:34.000 |   1 | 2022-03-16 22:31:33.999 |           5 |
           | +I | 2022-03-16 22:31:34.000 | 2022-03-16 22:31:36.000 |   1 | 2022-03-16 22:31:35.999 |           9 |
           | +I | 2022-03-16 22:31:34.000 | 2022-03-16 22:31:36.000 |   2 | 2022-03-16 22:31:35.999 |           9 |
        */

        /**
         * Grouping sets
         * Rollup
         * cube
         * 4/5/6/
         */
        /*tEnv.executeSql(SqlHolder.getSql(6)).print();*/


        /**
         *  over 查询
         *  range intervals
         *  row intervals
         */
        /*tEnv.executeSql(SqlHolder.getSql(8)).print();*/
        /*tEnv.executeSql(SqlHolder.getSql(9)).print();*/
        /*
         +----+------+-------------------------+--------+-------------+-------------+
         | op |   id |                 rowtime |  score |   sum_score |   avg_score |
         +----+------+-------------------------+--------+-------------+-------------+
         | +I |    1 | 2022-03-16 22:31:31.000 |      1 |           1 |           1 |
         | +I |    1 | 2022-03-16 22:31:32.000 |      2 |           3 |           1 |
         | +I |    1 | 2022-03-16 22:31:33.000 |      3 |           6 |           2 |
         | +I |    1 | 2022-03-16 22:31:34.000 |      4 |          10 |           2 |
         | +I |    1 | 2022-03-16 22:31:35.000 |      5 |          14 |           3 |
         | +I |    1 | 2022-03-16 22:31:36.000 |      6 |          18 |           4 |
       */


        /**
         * join
         *
         */
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9988);
        SingleOutputStreamOperator<Stu> s2 = source2.map(new MapFunction<String, Stu>() {
            @Override
            public Stu map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Stu(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Stu>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<Stu>() {
                    @Override
                    public long extractTimestamp(Stu element, long recordTimestamp) {
                        return element.getUpdateTime();
                    }
                }
        ));

        tEnv.createTemporaryView("t2",s2,Schema.newBuilder()
                .primaryKey("id")
                .columnByMetadata("update_time",DataTypes.TIMESTAMP_LTZ(3),"rowtime",true)
                .watermark("update_time",sourceWatermark())
                .build());

        /**
         * 模拟生成lookup表
         */
        /*tEnv.executeSql(SqlHolder.getSql(15)).print();*/

        /**
         * temporal join
         */
        /*tEnv.executeSql(SqlHolder.getSql(16));
        tEnv.executeSql(SqlHolder.getSql(17)).print();*/


        /**
         * lookup 表 join
         */
        tEnv.executeSql(SqlHolder.getSql(18));
        tEnv.executeSql(SqlHolder.getSql(19)).print();


        env.execute();
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class Stu implements Serializable {
        private String id;
        private String name;
        private long updateTime;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static  class Bean implements Serializable {
        private String id;
        private int score;
        private long ts;
    }
}