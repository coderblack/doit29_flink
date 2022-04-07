package cn.doitedu.flink.demos;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import cn.doitedu.flink.pojos.Student;

public class _08_MultipleStream_SplitStream2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        DataStream<Student> students = source.map(new Str2Student());


        // 测流标签在构造时，最好加上类型声明
        OutputTag<Student> maleOutputTag = new OutputTag<>("male", TypeInformation.of(Student.class));
        OutputTag<String> femaleOutputTag = new OutputTag<>("female",TypeInformation.of(String.class));

        SingleOutputStreamOperator<Student> mainStream = students.process(new ProcessFunction<Student, Student>() {
            @Override
            public void processElement(Student student, ProcessFunction<Student, Student>.Context ctx, Collector<Student> collector) throws Exception {
                if (student.getGender().equals("m")) {
                    // 输出到测流
                    ctx.output(maleOutputTag, student);
                } else if (student.getGender().equals("f")) {
                    // 输出到测流
                    ctx.output(femaleOutputTag, student.toString());
                } else {
                    // 在主流中输出
                    collector.collect(student);
                }
            }
        });

        /*mainStream.print();*/

        // 选择测流
        DataStream<Student> maleStudents = mainStream.getSideOutput(maleOutputTag);
        maleStudents.print();

        DataStream<String> femaleStudents = mainStream.getSideOutput(femaleOutputTag);




        env.execute();

    }

}
