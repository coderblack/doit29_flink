package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import pojos.Student;

public class _09_MultipleStream_connect {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.fromElements("1,aa,m,18","2,bb,m,28","3,cc,f,38");

        DataStreamSource<String> stream2 = env.fromElements("1:aa:m:18","2:bb:m:28","3:cc:f:38");


        ConnectedStreams<String, String> connect = stream1.connect(stream2);
        SingleOutputStreamOperator<Student> stream = connect.map(new CoMapFunction<String, String, Student>() {
            @Override
            public Student map1(String value) throws Exception {
                String[] split = value.split(",");
                return new Student(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
            }

            @Override
            public Student map2(String value) throws Exception {
                String[] split = value.split(":");
                return new Student(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
            }
        });
        stream.print();

        env.execute();

    }

}
