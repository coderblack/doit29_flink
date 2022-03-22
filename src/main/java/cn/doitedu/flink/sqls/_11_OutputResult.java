package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.$;

public class _11_OutputResult {

    public static void main(String[] args) throws Exception {

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


        // 注册 外部连接器 mysql 表，用来接收计算结果
        tenv.executeSql(SqlHolder.getSql(19));

        // 在查询sql中，用insert 语句，来将select结果插入到一个目标表（外部连接器表）
        /*tenv.executeSql("insert into flink_order " +
                "select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0");*/


        // 注册 外部连接器 kafka连接器表，用来接收appendOnly流  计算结果
        tenv.executeSql(SqlHolder.getSql(20));

        // 插入查询结果
        /*tenv.executeSql("insert into kafka_order " +
                        "select t1.f0 as id," +
                        "t1.f1 as order_time," +
                        "t2.f2 as product_name " +
                        "from t1 join t2 " +
                        "on t1.f0=t2.f0"
                );*/

        // 注册 外部连接器 upsert-kafka连接器表，用来接收计算结果
        tenv.executeSql(SqlHolder.getSql(21));
        // 插入查询结果
        /*tenv.executeSql("insert into kafka_upsert_order " +
                "select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0"
        );*/


        /**
         * 也可以把查询结果，转成 dataStream，然后用flink-core的api去做处理和输出
         */
        Table table = tenv.sqlQuery("select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0");
        // DataStream<Row> resDataStream = tenv.toDataStream(table);  // toDataStream 只能用来转换 “appendOnly流”
        DataStream<Row> resDataStream = tenv.toChangelogStream(table);// toChangelogStream 才能接受 "changeLog流 "

        SingleOutputStreamOperator<String> mapedChangeLogStream = resDataStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                RowKind changeKind = row.getKind(); // 变化标记  ; 拿到change标记后，可以判断 是哪一种变化，然后根据我们的需求进行后续的逻辑控制

                String id = (String) row.getField("id");
                long order_time = row.<Long>getFieldAs("order_time");
                String product_name = row.<String>getFieldAs("product_name");

                return changeKind.shortString() + " , " + changeKind.toByteValue() + " , " + id + " : " + order_time + " : " + product_name;
            }
        });


        mapedChangeLogStream.print();


        env.execute();
    }
}
