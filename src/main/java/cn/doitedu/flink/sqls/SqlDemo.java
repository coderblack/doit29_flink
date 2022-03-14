package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import pojos.Pojo;
import pojos.Student;

import javax.xml.crypto.Data;
import java.util.Arrays;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class SqlDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        SingleOutputStreamOperator<Student> s1 = source.map(s -> {
            String[] arr = s.split(",");
            return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
        }).returns(Student.class);

        Table table = tableEnv.fromDataStream(s1);
        tableEnv.createTemporaryView("t1",table);



        Table resultTable1 = tableEnv.sqlQuery("select * from t1 where gender = 'm'");
        Table resultTable2 = tableEnv.sqlQuery("select count(1) as cnt from t1 where gender = 'm'");


        /*tableEnv.toChangelogStream(resultTable1, Schema.newBuilder()
                .build(), ChangelogMode.all()
        ).map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                byte b = row.getKind().toByteValue();
                Integer id = row.<Integer>getFieldAs(0);
                String name = row.<String>getFieldAs(1);
                String gender = row.<String>getFieldAs(2);
                Double score = row.<Double>getFieldAs(3);
                return ""+b+","+new Pojo(id,name,gender,score);
            }
        }).print();*/


        tableEnv.toChangelogStream(resultTable2, Schema.newBuilder()
                .build(), ChangelogMode.all()
        ).map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                byte b = row.getKind().toByteValue();
                Long cnt = row.<Long>getFieldAs(0);
                return ""+b+","+cnt;
            }
        }).print();


        env.execute();


    }
}
