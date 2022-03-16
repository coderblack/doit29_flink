package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class SqlDemo3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // "1,zs,28", "2,ls,38", "3,ww,68"
        DataStreamSource<String> s1 = env.socketTextStream("localhost",9099);
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> stream = s1
                .map(s -> Tuple3.of(Integer.parseInt(s.split(",")[0]), s.split(",")[1], Integer.parseInt(s.split(",")[2])))
                .returns(new TypeHint<Tuple3<Integer, String, Integer>>() {
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Integer, String, Integer>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Integer,String,Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<Integer, String, Integer> element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                }));

        Table table = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("f0",DataTypes.INT())
                .column("f1",DataTypes.STRING())
                .column("f2",DataTypes.INT())

                /*.columnByExpression("name", $("f1").as("name"))
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")*/
                .build());

        Table table2 = table.select($("f1").as("name"), $("f0").as("id"), $("f2").as("age"));
        /*tableEnv.toDataStream(table2,DataTypes.of(TypeInformation.of(new TypeHint<Tuple3<String,Integer,Integer>>() {}))).print();*/

        Table table3 = table2.groupBy($("name")).aggregate($("age").sum().as("sum_age")).select($("name"), $("sum_age"));
        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(table3, Schema.newBuilder()
                .build(), ChangelogMode.all());

        /*tableEnv.createTemporaryView("t1",table);
        tableEnv.executeSql("select * from t1").print();*/

        tableEnv.executeSql("建表语句  with connector ");

        // 创建视图  --> 从dataStream,从table
        //tableEnv.createTemporaryView();
        tableEnv.executeSql("create table x1");

        // 创建表 --> 从dataStream,从视图,从table
        /*Table t2 = tableEnv.fromDataStream();
        Table t3 = tableEnv.from();
        Table t4 = tableEnv.fromChangelogStream(rowDataStream);*/


        TableDescriptor descriptor = TableDescriptor
                .forConnector("kafka")
                .format("csv")
                .build();
        Table t5 = tableEnv.from(descriptor);


        env.execute();

    }


}
