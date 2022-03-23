package cn.doitedu.flink.exercise;

import io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class mysql维表关联 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        DataStreamSource<String> source = env.socketTextStream("doit01", 9999);

        // g01,11000,pageview,p01
        SingleOutputStreamOperator<Tuple4<String, Long, String, String>> stream = source.map(new MapFunction<String, Tuple4<String, Long, String, String>>() {
            @Override
            public Tuple4<String, Long, String, String> map(String value) throws Exception {
                String[] arr = value.split(",");
                return Tuple4.of(arr[0], Long.parseLong(arr[1]), arr[2], arr[3]);
            }
        });

        // 将流，转成临时视图
        tenv.createTemporaryView("t_action_log",stream);

        // 将mysql的表，用连接器连接，注册成flink的表
        String mysqlTableDDL = "CREATE TABLE t_category (\n" +
                "  id STRING,  \n" +
                "  name STRING,  \n" +
                "  description  STRING   \n" +
                ") WITH (   \n" +
                "   'connector' = 'jdbc',  \n" +
                "   'url' = 'jdbc:mysql://localhost:3306/abc',   \n" +
                "   'table-name' = 'product_category'  , \n" +
                "   'username' = 'root'  , \n" +
                "   'password' = '123456'   \n" +
                ")";
        tenv.executeSql(mysqlTableDDL);

        // tenv.executeSql("select * from t_category").print();


        // 进行两表join
        tenv.executeSql("select f0,f1,f2,f3,t_category.name,t_category.description  from t_action_log join t_category on t_action_log.f3=t_category.id").print();


        env.execute();
    }


    public void joinDimension1(DataStreamSource<String> source){
        // g01,t1,pageview,p01
        SingleOutputStreamOperator<String> result = source.process(new ProcessFunction<String, String>() {
            PreparedStatement stmt;

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456");
                stmt = conn.prepareStatement("select name,description from product_category where id= ?");

            }

            // g01,t1,pageview,p01
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                String pid = value.split(",")[3];
                stmt.setString(1, pid);
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
                String pname = resultSet.getString("name");
                String description = resultSet.getString("description");

                out.collect(value + "," + pname + "," + description);
            }
        });

        result.print();
    }


}
