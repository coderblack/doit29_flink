package cn.doitedu.flink.apis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojos.Student;

public class ApiExercise_11_UnionStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // id,name,gender,score
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9098);

        // id:name:gender:score
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9099);

        SingleOutputStreamOperator<Student> s1 = source1.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
            }
        });

        SingleOutputStreamOperator<Student> s2 = source2.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] arr = value.split(":");
                return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
            }
        });

        DataStream<Student> unioned2 = s1.union(s2);

        unioned2.filter(s->s.getScore()>60).print();


        env.execute();


    }
}
