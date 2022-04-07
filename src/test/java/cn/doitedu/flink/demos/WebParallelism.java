package cn.doitedu.flink.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import cn.doitedu.flink.pojos.Student;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/8
 **/
public class WebParallelism {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        SingleOutputStreamOperator<String> s1 = env.socketTextStream("localhost", 9099).name("s1");

        SingleOutputStreamOperator<Student> s2 = s1.map(new Str2Student()).name("s2").disableChaining();

        SingleOutputStreamOperator<Student> s3 = s2.map(s -> {
            s.setScore(s.getScore() * 10);
            return s;
        }).returns(Student.class).name("s3");

        KeyedStream<Student, String> keyed = s3.keyBy(s -> s.getGender());

        SingleOutputStreamOperator<Student> s4 = keyed.minBy("score").name("s4");
        SingleOutputStreamOperator<Student> s5 = s4.map(s -> {
            s.setGender(s.getGender() + " h");
            return s;
        }).name("s5").startNewChain();

        SingleOutputStreamOperator<Student> s6 = s5.map(s -> {
            s.setId(s.getId() + 100);
            return s;
        }).name("s6");

        s6.print().name("s7");

        env.execute();

    }
}
