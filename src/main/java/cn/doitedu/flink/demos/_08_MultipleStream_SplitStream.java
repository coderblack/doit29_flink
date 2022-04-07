package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import cn.doitedu.flink.pojos.Student;

public class _08_MultipleStream_SplitStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        DataStream<Student> students = source.map(new Str2Student());

        // 按性别，将students数据流，分成2个流
        DataStream<Student> maleStudents = students.filter(s -> s.getGender().equals("m"));
        DataStream<Student> femaleStudents = students.filter(s -> s.getGender().equals("f"));

        maleStudents.map(s->{
            s.setName(s.getName().toUpperCase());
            return s;
        }).returns(Student.class).print();


        femaleStudents.map(s->{
            s.setScore(s.getScore()*10);
            return s;
        }).returns(Student.class).print();

        env.execute();
    }
}
