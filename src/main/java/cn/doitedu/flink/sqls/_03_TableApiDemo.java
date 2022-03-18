package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class _03_TableApiDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<UserAction> stream = source.map(new MapFunction<String, UserAction>() {
            @Override
            public UserAction map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserAction(Long.parseLong(split[0]), Long.parseLong(split[1]), split[2], Long.parseLong(split[3]));
            }
        });


        // 将一个已存在的视图，转成table对象
        // Table table1 = tenv.from("view_x");

        // 从流转成Table对象
        Table table2 = tenv.fromDataStream(stream); // 从流转表，不指定schema，则直接使用底层物理字段名、字段类型
        table2.printSchema();  // 打印表结构信息

        Table table3 = tenv.fromDataStream(stream, Schema.newBuilder().build());// 从流转表对象，利用Schema工具指定表字段定义信息
        table3.printSchema();

        // 测试用的生成表对象的方法
        Table table4 = tenv.fromValues(1, 2, 3, 4, 5, 6);// fromValues，纯粹用于做测试，方便获得表对象  f0:Int


        // 通过表描述器，来生成表对象(底层逻辑就是指定connector + schema)
        Table table5 = tenv.from(TableDescriptor.forConnector("kafka")
                .option("topic", "flinksql-01")
                .option("properties.bootstrap.servers", "doit01:9092")
                .option("properties.group.id", "gx01")
                .option("scan.startup.mode", "latest-offset")
                .format("csv")
                .schema(Schema.newBuilder().build())
                .build());

        /**
         * 针对table对象的tableApi
         * select upper(eventId) as eventIdUpper from t;
         */
        table3.printSchema(); // 打印表结构
        Table resTable1 = table3.select("guid,actionTime,eventId");
        Table resTable2 = table3.select($("guid"), $("actionTime"), $("eventId").upperCase().as("eventIdUpper"));
        Table resTable3 = resTable2.where($("actionTime").isGreater(lit(2)));

        resTable3.execute().print(); // 打印结果
        resTable3.executeInsert("view_target"); // 把resTable3的结果查询出来并插入一张目标表（映射了外部数据源的视图）
        resTable3.executeInsert(TableDescriptor.forConnector("kafka")
                .option("topic", "flinksql-01")
                .option("properties.bootstrap.servers", "doit01:9092")
                .option("properties.group.id", "gx01")
                .option("scan.startup.mode", "latest-offset")
                .format("csv")
                .schema(Schema.newBuilder().build())
                .build());   // 把resTable3的结果查询出来，并插入一个用表描述器所描述的外部数据源

    }
}
