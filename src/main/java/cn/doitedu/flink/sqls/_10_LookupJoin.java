package cn.doitedu.flink.sqls;

import cn.doitedu.flink.utils.SqlHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

import static org.apache.flink.table.api.Expressions.$;

public class _10_LookupJoin {

    public static void main(String[] args) throws IOException {

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


        // 建事实流表 : t1
        tenv.createTemporaryView("t1",stream1, Schema.newBuilder()
                .columnByExpression("uid",$("f0"))
                .columnByExpression("actTime",$("f1"))
                .columnByExpression("pid",$("f2"))
                .columnByExpression("ptime","proctime()")  // 表达式逻辑字段，必须引用 物理字段
                .build());



        // 把mysql中的维表 product_category ，注册成flinksql中的表 product_category
        tenv.executeSql(SqlHolder.getSql(17));


        // 两表 lookup join
        /**
         * SELECT
         *    t1.uid,t1.actTime,t1.pid, c.name,c.description
         * FROM t1
         * JOIN product_category FOR SYSTEM_TIME AS OF t1.ptime AS c
         * ON t1.pid = c.id
         */
        tenv.executeSql(SqlHolder.getSql(18)).print();



    }

}
