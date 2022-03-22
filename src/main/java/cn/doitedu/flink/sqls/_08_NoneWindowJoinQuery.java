package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class _08_NoneWindowJoinQuery {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // a,1647915750000,1
        DataStreamSource<String> s1 = env.socketTextStream("doit01", 9998);
        SingleOutputStreamOperator<Tuple3<String, Long,String>> stream1 = s1.map(new MapFunction<String, Tuple3<String, Long,String>>() {
            @Override
            public Tuple3<String, Long,String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Long.parseLong(split[1]),split[2]);
            }
        });


        // a,1647915750000,x
        DataStreamSource<String> s2 = env.socketTextStream("doit02", 9999);
        SingleOutputStreamOperator<Tuple3<String, Long,String>> stream2 = s2.map(new MapFunction<String, Tuple3<String, Long,String>>() {
            @Override
            public Tuple3<String, Long,String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], Long.parseLong(split[1]),split[2]);
            }
        });


        tenv.createTemporaryView("t1", stream1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.BIGINT())
                .column("f2", DataTypes.STRING())
                .columnByExpression("rt", "to_timestamp_ltz(f1,3)")
                .watermark("rt", "rt")  // 将 rt 字段声明为事件时间
                .build());


        tenv.createTemporaryView("t2", stream2, Schema.newBuilder()
                .column("f0", DataTypes.STRING().notNull())  // 将f0字段声明为 非空 ，因为要将它当主键
                .column("f1", DataTypes.BIGINT())
                .column("f2", DataTypes.STRING())
                .columnByExpression("rt", "to_timestamp_ltz(f1,3)")
                .watermark("rt", "rt")  // 将 rt 字段声明为事件时间
                .primaryKey("f0")  // 将f0声明成主键，用来支撑 temporal join
                .build());

        // 常规 inner join
        tenv.executeSql("select * from t1 join t2 on t1.f0 = t2.f0")/*.print()*/;

        // 常规  left join
        tenv.executeSql("select * from t1 left join t2 on t1.f0 = t2.f0")/*.print()*/;

        // 常规 right join
        tenv.executeSql("select * from t1 right join t2 on t1.f0 = t2.f0")/*.print()*/;

        // 常规 full join
        tenv.executeSql("select * from t1 full outer join t2 on t1.f0 = t2.f0")/*.print()*/;


        // 时间范围join:  interval join
        // 小细节：两表join时，如果都有 event-time属性，则最后的结果不能同时 select 两表的两个 event-time字段
        tenv.executeSql("select t1.f0, t1.f1, t1.f2, t1.rt, t2.f2 from t1,t2 where t1.f0=t2.f0 and t1.rt between t2.rt - interval '5' second and t2.rt ")/*.print()*/;

        // 世代join:  temporal join
        // 小细节：两表join时，如果都有 event-time属性，则最后的结果不能同时 select 两表的两个 event-time字段
        tenv.executeSql("select t1.f0,t1.f2,t1.rt,t2.f0 as t2_f0,t2.f2 as t2_f2 from t1 " +
                "LEFT JOIN t2 FOR SYSTEM_TIME AS OF t1.rt " +
                "ON t1.f0 = t2.f0").print();

    }
}
