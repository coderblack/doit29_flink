package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.Student;
import cn.doitedu.flink.pojos.StudentStat;

public class _07_KeyByAverage {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        // 把字符串转成 Student对象，并按性别分组
        KeyedStream<Student, String> keyed = source.map(new Str2Student())
                .keyBy(s -> s.getGender());

        // 求分数的最小值，最大值，平均值，总人数
        keyed.process(new KeyedProcessFunction<String, Student, StudentStat>() {
            double sum = 0;
            int cnt = 0;
            double minScore = -1;
            double maxScore = 0;
            @Override
            public void processElement(Student value, KeyedProcessFunction<String, Student, StudentStat>.Context ctx, Collector<StudentStat> out) throws Exception {
                // 从上下文中获取当前正在处理的数据所属的key（组）
                String currentKey = ctx.getCurrentKey();

                // 从当前进来的一条数据中取 分数
                sum += value.getScore();
                // 对本组中的数据条数（人数）增加
                cnt++;

                // 判断当前数据是否比之前的最小值还小
                if(value.getScore()< minScore || minScore==-1) minScore = value.getScore();

                // 更新最大值
                if(value.getScore()>maxScore) maxScore = value.getScore();

                // 返回平均分结果
                out.collect(new StudentStat(currentKey,minScore,maxScore,sum/cnt,cnt));
            }
        }).print();

        env.execute();

    }
}
