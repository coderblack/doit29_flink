package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.Expression;
import static org.apache.flink.table.api.Expressions.$;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class SqlDemo2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // stream -> table
        DataStreamSource<String> s1 = env.fromElements("1,zs,28", "2,ls,38", "3,ww,68");
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> stream = s1
                .map(s -> Tuple3.of(Integer.parseInt(s.split(",")[0]), s.split(",")[1], Integer.parseInt(s.split(",")[2])))
                .returns(new TypeHint<Tuple3<Integer, String, Integer>>() {
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Integer, String, Integer>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Integer,String,Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<Integer, String, Integer> element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                }));

        /*Table table = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("f0",DataTypes.INT())
                .column("f1",DataTypes.STRING())
                .column("f2",DataTypes.INT())
                .columnByExpression("name", $("f1").as("name"))
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                .build());*/

        /*Table table2 = table.select($("f1").as("name"), $("f0").as("id"), $("f2").as("age"));*/
        /*tableEnv.toDataStream(table2,DataTypes.of(TypeInformation.of(new TypeHint<Tuple3<String,Integer,Integer>>() {}))).print();*/

        // tableEnv.createTemporaryView("stu",stream,"id,name,age");
        //tableEnv.createTemporaryView("stu",stream,"id,name,age");
        tableEnv.createTemporaryView("stu",stream, Schema.newBuilder()
                /*.column("f0",DataTypes.INT())
                .column("f1",DataTypes.STRING())
                .column("f2",DataTypes.INT())*/
                .columnByExpression("id",$("f0").as("id"))
                .columnByExpression("name",$("f1").as("name"))
                .columnByExpression("age",$("f2").as("age"))
                .columnByMetadata("rowtime",DataTypes.TIMESTAMP_LTZ(3),"rowtime",true)
                .build());


        String sql_1 = "select * from stu";
        String sql_2 = "select id,name,age from stu";
        String sql_3 = "select * from stu";
        /*tableEnv.sqlQuery(sql_3).execute().print();*/

        tableEnv.from("stu").execute().print();

        env.execute();

    }
}
