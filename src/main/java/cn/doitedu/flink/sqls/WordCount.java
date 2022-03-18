package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 类似于spark中 sparkContext，得到 rdd
        // StreamExecutionEnvironment,得到的是 stream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String,Integer>> s1 = source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String w : arr) {
                    out.collect(Tuple2.of(w,1));
                }
            }
        });


        /**
         * 第1步曲：把数据映射成表
         */
        // 把 流变成 tableApi中的表对象
        Table table = tEnv.fromDataStream(s1, "word,cnt");  // tableApi
        // table.select().groupBy()

        // 把 流变成sql中的视图（表）
        // 在sql上下文环境的元数据中，就拥有了一张表名： t1
        tEnv.createTemporaryView("t1",s1);   // 将流注册成表，且用默认生成的Schema ==>  f0:String ，f1:Int
        //tEnv.executeSql("desc t1").print();  // 查询 t1的表结构（Schema）
        /*
           +------+--------+-------+-----+--------+-----------+
           | name |   type |  null | key | extras | watermark |
           +------+--------+-------+-----+--------+-----------+
           |   f0 | STRING |  true |     |        |           |
           |   f1 |    INT | false |     |        |           |
           +------+--------+-------+-----+--------+-----------+
        */

        tEnv.createTemporaryView("t2",s1,"word,cnt");  // 将流注册成表，用字符串形式且手动指定Schema中的字段名
        //tEnv.executeSql("desc t2").print();  // 查询 t2的表结构（Schema）
        /*
            +------+--------+------+-----+--------+-----------+
            | name |   type | null | key | extras | watermark |
            +------+--------+------+-----+--------+-----------+
            | word | STRING | true |     |        |           |
            |  cnt |    INT | true |     |        |           |
            +------+--------+------+-----+--------+-----------+
        */
        tEnv.createTemporaryView("t3",s1,$("word"),$("cnt"));  // 将流注册成表，用表达式形式且手动指定Schema中的字段名
        //tEnv.executeSql("desc t3").print();  // 查询 t2的表结构（Schema）


        /**
         * 第2步曲，写sql逻辑
         */
        // tEnv.executeSql("select * from t2").print();
        tEnv.executeSql("select word,sum(cnt)  as cnt from t2 group by word").print();



        // 代码中用到了stream的算子，就需要加上evn.execute()；否则不用加
        env.execute();

    }
}
