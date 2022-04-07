package cn.doitedu.flink.apis;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.StuInfo;
import cn.doitedu.flink.pojos.Student;

public class ApiExercise_12_JoinedStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // id,name,gender,score
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9098);

        // id:phone,city
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9099);


        SingleOutputStreamOperator<Student> s1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
        }).returns(Student.class);


        SingleOutputStreamOperator<StuInfo> s2 = source2.map(s -> {
            String[] arr = s.split(":");
            return new StuInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });

        JoinedStreams<Student, StuInfo> joined = s1.join(s2);

        DataStream<String> stream = joined
                // where 流1的某字段  equalTo 流2的某字段
                .where(s -> s.getId()).equalTo(s -> s.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Student, StuInfo, String>() {
                    // 这边传入的两个流的两条数据，是能够满足关联条件的
                    @Override
                    public String join(Student first, StuInfo second) throws Exception {
                        return first.getId() + "," + first.getName() + "," + first.getGender() + "," + first.getScore() + "," + second.getId() + "," + second.getPhone() + "," + second.getCity();
                    }
                });

        joined.where(s -> s.getId()).equalTo(s -> s.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new FlatJoinFunction<Student, StuInfo, String>() {
                    @Override
                    public void join(Student first, StuInfo second, Collector<String> out) throws Exception {
                        out.collect(first.getId() + "," + first.getName() + "," + first.getGender() + "," + first.getScore() + "," + second.getId() + "," + second.getPhone() + "," + second.getCity());
                        ;
                        out.collect(first.getId() + "," + first.getName() + "," + first.getGender() + "," + first.getScore() + "," + second.getId() + "," + second.getPhone() + "," + second.getCity());
                        ;
                    }
                });


        stream.print();

        env.execute();
    }


}
