package cn.doitedu.flink.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

public class _06_KeyByAggregation_Demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // "id,name,gender,score"
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        // 把数据变成Student对象数据，方便后续的处理
        DataStream<Tuple4<Integer, String, String, Double>> students = source.map(new Str2Tuple());


        /**
         * 聚合类算子，一定是在KeyBy后的流（KeyedStream）上才能调用
         */
        KeyedStream<Tuple4<Integer, String, String, Double>, String> keyed = students.keyBy(tp -> tp.f2);

        // sum  每种性别的同学的总分
        keyed.sum(3)/*.print()*/;  // 返回的结果，依然是源头的类型Tuple4，那么聚合后的结果中，非分组字段和聚合字段，直接取该组中进来的一条的数据


        // min  每种性别的同学的最低分
        keyed.min(3)/*.print()*/;

        // max  每种性别的同学的最高分
        keyed.max(3)/*.print()*/;

        // minBy  // 按找指定字段求最小值，并且会返回最小值所在的整条数据
        keyed.minBy(3)/*.print()*/;

        // maxBy
        keyed.maxBy(3)/*.print()*/;

        // reduce  每种性别的同学的总分
        keyed.reduce(new ReduceFunction<Tuple4<Integer, String, String, Double>>() {
            @Override
            public Tuple4<Integer, String, String, Double> reduce(Tuple4<Integer, String, String, Double> value1, Tuple4<Integer, String, String, Double> value2) throws Exception {
                value2.f3 += value1.f3;
                return value2;
            }
        })/*.print()*/;

        // reduce  将条数据不断拼接字符串
        source.keyBy(s -> s.split(",")[2])
                .reduce(new ReduceFunction<String>() {
                            @Override
                            public String reduce(String value1, String value2) throws Exception {
                                return value1 + "|" + value2;
                            }
                        }
                ).print();


        // TODO 随堂练习
        // 求 所有人中的 最高分，最低分，分数之和，平均分，到目前为止的人数
        KeyedStream<Tuple4<Integer, String, String, Double>, String> globalKeyed = students.keyBy(new KeySelector<Tuple4<Integer, String, String, Double>, String>() {
            @Override
            public String getKey(Tuple4<Integer, String, String, Double> value) throws Exception {
                return "a";   // 返回常量，就会导致任意一条数据都会被分到同一组
            }
        });
        globalKeyed.max(3)/*.print()*/;
        globalKeyed.min(3)/*.print()*/;
        globalKeyed.sum(3)/*.print()*/;

        // 求数据总条数
        SingleOutputStreamOperator<Tuple1<Integer>> ss = students.map(s -> Tuple1.of(1)).returns(new TypeHint<Tuple1<Integer>>() {
        });
        ss.keyBy(0)
          .sum(0)
          .print();


        env.execute();
    }
}

class Student implements Serializable {
    private int id;
    private String name;
    private String gender;
    private double score;

    public Student() {
    }

    public Student(int id, String name, String gender, double score) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.score = score;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", score=" + score +
                '}';
    }
}

class Str2Student implements MapFunction<String, Student> {
    @Override
    public Student map(String value) throws Exception {
        String[] arr = value.split(",");
        return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
    }
}


class Str2Tuple implements MapFunction<String, Tuple4<Integer, String, String, Double>> {
    @Override
    public Tuple4<Integer, String, String, Double> map(String value) throws Exception {
        String[] arr = value.split(",");
        return Tuple4.of(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
    }
}