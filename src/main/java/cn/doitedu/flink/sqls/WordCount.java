package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
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
         * 新版本api，用Schema来定义表结构时，物理字段（流中通过类型信息解构出来的字段，是默认自动存在于表结构中）f0,f1
         * 如果想声明跟底层解构出来的字段信息不同的定义, 那得额外添加新的逻辑字段（逻辑字段，是可以用表达式引用物理字段来生成）
         */
        tEnv.createTemporaryView("t4",s1, Schema.newBuilder()
                        .column("f0", DataTypes.STRING())  // column指定物理字段名，相当于啥也没做，不过可以指定 字段的类型
                        .column("f1", DataTypes.INT())  //
                        .columnByExpression("word",$("f0").upperCase())  // 声明一个新的逻辑字段，它名字叫word，它来自于底层f0字段的运算结果   create table t_x( f0 string,f1 int , word as upper(f0) ) ;
                        .columnByExpression("cnt",$("f1").cast(DataTypes.BIGINT()))  // 声明一个新的逻辑字段，它名字叫cnt，它来自于底层f1字段，并进行了类型转换
                        .columnByExpression("cnt2","cast(f1 as bigint)")  // 声明一个新的逻辑字段，名字叫cnt2，来自于底层物理字段f1,而且表达式用sql字符串表达式
                        .columnByMetadata("rt",DataTypes.TIMESTAMP_LTZ(3),"rowtime")    // 从底层数据源（流，或者connector）中抽取某个元数据，声明成一个表的逻辑字段
                .build());
        tEnv.executeSql("desc t4").print();
        /**
         * +------+--------+------+-----+-----------------------+-----------+
         * | name |   type | null | key |                extras | watermark |
         * +------+--------+------+-----+-----------------------+-----------+
         * |   f0 | STRING | true |     |                       |           |
         * |   f1 |    INT | true |     |                       |           |
         * | word | STRING | true |     |          AS upper(f0) |           |
         * |  cnt | BIGINT | true |     |   AS cast(f1, BIGINT) |           |
         * | cnt2 | BIGINT | true |     | AS cast(f1 as bigint) |           |
         * +------+--------+------+-----+-----------------------+-----------+
         */

        tEnv.executeSql("select * from t4").print();




        /**
         * 第2步曲，写sql逻辑
         */
         tEnv.executeSql("select * from t2")/*.print()*/;
        // 输出的结果表，只包含Insert模式的数据，属于AppendOnly表（append流）

        tEnv.executeSql("select word,sum(cnt)  as cnt from t2 group by word")/*.print()*/;
        /**
         * 输出的结果表，在底层是一个 updated table（changelog流)
         * +----+--------------------------------+-------------+
         * | op |                           word |         cnt |
         * +----+--------------------------------+-------------+
         * | +I |                              a |           1 |
         * | -U |                              a |           1 |
         * | +U |                              a |           2 |
         * | -U |                              a |           2 |
         * | +U |                              a |           3 |
         * | +I |                              b |           1 |
         */


        // 代码中用到了stream的算子，就需要加上evn.execute()；
        // 否则不用加
        env.execute();

    }
}
