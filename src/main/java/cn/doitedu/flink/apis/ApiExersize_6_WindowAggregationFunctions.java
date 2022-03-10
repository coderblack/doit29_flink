package cn.doitedu.flink.apis;

import cn.doitedu.flink.functions.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojos.Score;

import java.time.Duration;

/**
 * 窗口聚合函数大合集
 */
public class ApiExersize_6_WindowAggregationFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.addSource(new Ex6_DataSource());

        SingleOutputStreamOperator<Score> stream2 = stream1
                .map(new Ex6_Str2ScoreMapFunc())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Score>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));

        KeyedStream<Score, String> keyedStream = stream2.keyBy(s -> s.getGender());

        // 计数窗口聚合
        keyedStream.countWindow(5, 2)
                /*.max("score") */  // 得到的结果中，除了score是符合逻辑的结果外，其他字段是不可预料的，一直在更新
                /*.min("score")*/
                /*.maxBy("score") */ // 得到的结果是： 最大score所在的那一行数据
                /*.minBy("score")*/  // 得到的结果是： 最小score所在的那一行数据
                /*.sum("score")*/  // 得到的结果中，除了score是符合逻辑(score之和)的结果外，其他字段是不可预料的，一直在更新
                /*.reduce(new Ex6_ReduceFunc1())  */
                /*.aggregate(new Ex6_AvgScoreAggrFunc())*/   // 自定义聚合函数（可以自己控制中间累加器的结构和更新逻辑，以及控制最终结果的生成逻辑）
                /*.apply(new Ex6_AvgScoreWindowFunc())*/  // 自定义一个全窗口聚合函数（特点是，会在窗口触发的时候把窗口中的所有数据一次性传给函数，同时附带了窗口信息）
                .process(new ProcessWindowFunction<Score, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Score, String, String, GlobalWindow>.Context context, Iterable<Score> elements, Collector<String> out) throws Exception {
                        int cnt = 0;
                        double sumScore = 0.0;
                        for (Score element : elements) {
                            cnt++;
                            sumScore += element.getScore();
                        }
                        out.collect(String.format("平均分为： %.2f ", sumScore));
                    }
                })
                .print();


        // 时间（EventTime)窗口聚合
        // 分数之积
        keyedStream.window(TumblingEventTimeWindows.of(Time.milliseconds(3000)))
                /*.min("score")*/
                /*.max("score")*/
                /*.minBy("score")*/
                /*.maxBy("score")*/
                /*.sum("score")*/
                /*.reduce((ReduceFunction<Score>) (value1, value2) -> {
                    value2.setScore(value1.getScore() * value2.getScore());
                    return value2;
                }).returns(Score.class)*/
                /*.aggregate(new AggregateFunction<Score, Score, Score>() {   // 求窗口内的所有数据中分数最高的那一条
                    @Override
                    public Score createAccumulator() {
                        return null;
                    }

                    @Override
                    public Score add(Score value, Score accumulator) {
                        if(accumulator == null) {  // 第一次来数据，让累加直接等于这条数据
                            accumulator = value;
                        }else{
                            // 判断当前数据的分数是否>累加器（上一次的人的分数）,就替换掉累加器的数据为当前数据
                            if(accumulator.getScore() < value.getScore()) accumulator = value;
                        }
                        return accumulator;
                    }

                    @Override
                    public Score getResult(Score accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Score merge(Score a, Score b) {
                        return a.getScore()>b.getScore()?a:b;
                    }
                })*/
                /*.apply(new WindowFunction<Score, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Score> input, Collector<String> out) throws Exception {
                        int cnt = 0;
                        double sumScore = 0.0;
                        for (Score score : input) {
                            cnt ++;
                            sumScore += score.getScore();
                        }
                        out.collect(String.format("我就是一个神,我掐指一算，本窗口的平均成绩为：%.2f",sumScore/cnt));
                    }
                })*/
                .process(new ProcessWindowFunction<Score, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Score, String, String, TimeWindow>.Context context, Iterable<Score> input, Collector<String> out) throws Exception {
                        int cnt = 0;
                        double sumScore = 0.0;
                        for (Score score : input) {
                            cnt++;
                            sumScore += score.getScore();
                        }
                        out.collect(String.format("吾乃涛神,本仙掐指一算，本窗口的平均成绩为：%.2f", sumScore / cnt));
                    }

                });


        env.execute();
    }
}
