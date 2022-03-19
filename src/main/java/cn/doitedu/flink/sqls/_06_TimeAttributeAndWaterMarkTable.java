package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.sourceWatermark;

/**
 * 声明event-time属性，及 WaterMark策略
 * <p>
 * 声明 processing-time属性
 */
public class _06_TimeAttributeAndWaterMarkTable {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * sql 建表语句中指定字段（物理字段、逻辑表达式字段、元数据逻辑字段）为 Event-time及 watermark
         */
        // tenv.executeSql(SqlHolder.getSql(10));
        // tenv.executeSql("select guid,action_time,event_id,action_timelong,channel,action_time_ltz, current_watermark(action_time_ltz)  from action_event")/*.print()*/;


        /**
         * tableApi 中指定字段为event-time及watermark策略
         */
        TableDescriptor descriptor = TableDescriptor.forConnector("kafka")
                .option("topic", "flinksql-01")
                .option("properties.bootstrap.servers", "doit01:9092")
                .option("properties.group.id", "testGroup")
                .option("scan.startup.mode", "latest-offset")
                .option("format", "csv")
                .option("csv.ignore-parse-errors", "true")
                .schema(Schema.newBuilder()
                        .column("guid", DataTypes.BIGINT())
                        .column("action_time", DataTypes.BIGINT())  // 用户行为发生时间
                        .column("event_id", "STRING")
                        .column("action_timelong", "BIGINT")
                        .column("channel", "STRING")
                        .columnByExpression("action_time_ltz", "to_timestamp_ltz(action_time,3)") // 得到一个表达式逻辑字段
                        .watermark("action_time_ltz", "action_time_ltz - interval '5' second")  //声明 watermark
                        .build())
                .format("csv")
                .build();
        Table table = tenv.from(descriptor);
        /*table.printSchema();
        Table resTable = table.select($("guid"));*/
        //tenv.createTemporaryView("t_action",table);


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<UserAction> stream = source.map(new MapFunction<String, UserAction>() {
            @Override
            public UserAction map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserAction(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], Long.parseLong(split[3]), split[4]);
            }
        });

        /**
         * 将stream生成table对象的过程中 ，定义event-time和watermark
         */
        // 物理字段 必须跟 流中的pojo属性名相同
        Table table2 = tenv.fromDataStream(stream, Schema.newBuilder()
                .column("guid", DataTypes.BIGINT())
                .column("actionTime", DataTypes.BIGINT())  // 用户行为发生时间
                .column("eventId", "STRING")
                .column("actionStaylong", "BIGINT")
                .column("channel", "STRING")
                .columnByExpression("action_time_ltz", "to_timestamp_ltz(actionTime,3)") // 得到一个表达式逻辑字段
                .watermark("action_time_ltz", "action_time_ltz - interval '5' second")  //声明 watermark
                .build());
        tenv.createTemporaryView("t_action2", table2);  // 把 table 对象注册成sql表


        /**
         * 将 带有event-time和 watermark的 流， 转成 table 时，指定表的event-time
         */
        SingleOutputStreamOperator<UserAction> stream2 = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserAction>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserAction>() {
            @Override
            public long extractTimestamp(UserAction element, long recordTimestamp) {
                return element.getActionTime();
            }
        }));

        tenv.createTemporaryView("t_action3", stream2, Schema.newBuilder()
                .column("guid", DataTypes.BIGINT())
                .column("actionTime", DataTypes.BIGINT())  // 用户行为发生时间
                .column("eventId", "STRING")
                .column("actionStaylong", "BIGINT")
                .column("channel", "STRING")
                // 这一次，stream2流中，已经带有了事件时间属性和watermark，那么就不需要如下显示声明！
                /*.columnByExpression("action_time_ltz", "to_timestamp_ltz(actionTime,3)")
                .watermark("action_time_ltz", "action_time_ltz - interval '5' second")   */
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("rt", sourceWatermark())  // sourceWatermark()只是一个标记函数，表示：沿用物理流中的watermark并往下游传递
                .build());


        /**
         * 声明processing-time
         * 在sql中:   `pt` as proctime(), -- 用一个表达式逻辑字段来声明processing-time属性，其中的表达式函数 proctime()，仅仅是一个标记函数
         * 完整示例在 sql_10 中
         *
         * 在tableApi中，如下：
         */
        tenv.createTemporaryView("t_action4",stream,
                Schema.newBuilder()
                        .column("guid", DataTypes.BIGINT())
                        .column("actionTime", DataTypes.BIGINT())  // 用户行为发生时间
                        .column("eventId", "STRING")
                        .column("actionStaylong", "BIGINT")
                        .column("channel", "STRING")
                        .columnByExpression("pt","proctime()")  // 声明一个表达式逻辑字段，并标记为processing-time属性
                        .columnByExpression("action_time_ltz", "to_timestamp_ltz(actionTime,3)") // 得到一个表达式逻辑字段
                        .watermark("action_time_ltz", "action_time_ltz - interval '5' second")  //声明 event-time及 watermark
                        .build());
        tenv.executeSql("desc t_action4").print();
        /**
         * +-----------------+-----------------------------+-------+-----+-----------------------------------+---------------------------------------+
         * |            name |                        type |  null | key |                            extras |                             watermark |
         * +-----------------+-----------------------------+-------+-----+-----------------------------------+---------------------------------------+
         * |            guid |                      BIGINT |  true |     |                                   |                                       |
         * |      actionTime |                      BIGINT |  true |     |                                   |                                       |
         * |         eventId |                      STRING |  true |     |                                   |                                       |
         * |  actionStaylong |                      BIGINT |  true |     |                                   |                                       |
         * |         channel |                      STRING |  true |     |                                   |                                       |
         * |              pt | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |                     AS proctime() |                                       |
         * | action_time_ltz |  TIMESTAMP_LTZ(3) *ROWTIME* |  true |     | AS to_timestamp_ltz(actionTime,3) | action_time_ltz - interval '5' second |
         * +-----------------+-----------------------------+-------+-----+-----------------------------------+---------------------------------------+
         */

    }
}
