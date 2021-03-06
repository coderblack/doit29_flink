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
                .watermark("rt", "rt")  // ??? rt ???????????????????????????
                .build());


        tenv.createTemporaryView("t2", stream2, Schema.newBuilder()
                .column("f0", DataTypes.STRING().notNull())  // ???f0??????????????? ?????? ???????????????????????????
                .column("f1", DataTypes.BIGINT())
                .column("f2", DataTypes.STRING())
                .columnByExpression("rt", "to_timestamp_ltz(f1,3)")
                .watermark("rt", "rt")  // ??? rt ???????????????????????????
                .primaryKey("f0")  // ???f0?????????????????????????????? temporal join
                .build());


        // ?????? ??????????????? mysql ??????????????????????????????
        tenv.executeSql(SqlHolder.getSql(19));

        // ?????????sql?????????insert ???????????????select??????????????????????????????????????????????????????
        /*tenv.executeSql("insert into flink_order " +
                "select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0");*/


        // ?????? ??????????????? kafka???????????????????????????appendOnly???  ????????????
        tenv.executeSql(SqlHolder.getSql(20));

        // ??????????????????
        /*tenv.executeSql("insert into kafka_order " +
                        "select t1.f0 as id," +
                        "t1.f1 as order_time," +
                        "t2.f2 as product_name " +
                        "from t1 join t2 " +
                        "on t1.f0=t2.f0"
                );*/

        // ?????? ??????????????? upsert-kafka???????????????????????????????????????
        tenv.executeSql(SqlHolder.getSql(21));
        // ??????????????????
        /*tenv.executeSql("insert into kafka_upsert_order " +
                "select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0"
        );*/


        /**
         * ????????????????????????????????? dataStream????????????flink-core???api?????????????????????
         */
        Table table = tenv.sqlQuery("select t1.f0 as id," +
                "t1.f1 as order_time," +
                "t2.f2 as product_name " +
                "from t1 full join t2 " +
                "on t1.f0=t2.f0");
        // DataStream<Row> resDataStream = tenv.toDataStream(table);  // toDataStream ?????????????????? ???appendOnly??????
        DataStream<Row> resDataStream = tenv.toChangelogStream(table);// toChangelogStream ???????????? "changeLog??? "

        SingleOutputStreamOperator<String> mapedChangeLogStream = resDataStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                RowKind changeKind = row.getKind(); // ????????????  ; ??????change???????????????????????? ???????????????????????????????????????????????????????????????????????????

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
