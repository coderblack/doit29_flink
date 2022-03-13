package cn.doitedu.flink.apis;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import pojos.Student;

public class ApiExercise_10_ConnectedStream {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // id,name,gender,score
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9098);
        // id:name:gender:score
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9099);

        // 连接两条流
        ConnectedStreams<String, String> connected = source1.connect(source2);

        // 连接后的流做map操作，传入的是CoMapFunction，里面有2个map方法
        SingleOutputStreamOperator<Student> stream = connected.map(new CoMapFunction<String, String, Student>() {
            @Override
            public Student map1(String value) throws Exception {
                String[] arr = value.split(",");
                return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
            }

            @Override
            public Student map2(String value) throws Exception {
                String[] arr = value.split(":");
                return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
            }
        });

        stream.filter(s->s.getScore()>60).print();

        env.execute();

    }


}
