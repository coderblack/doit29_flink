package cn.doitedu.flink.apis;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojos.Score;

import java.time.Duration;

/**
 * 测试水位线上下游推进机制
 * 下游取上游的所有分区task的水位线的最小值作为当前自己的时间推进点
 */
public class ApiExercise_04_WaterMarkForward {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1,a,100,1646784001000
        // 1,a,100,1646784002000
        // 1,b,100,1646784001000
        // 1,b,100,1646784002000
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);  // 并行度只能为1

        SingleOutputStreamOperator<Score> stream1 = source.map(line -> {
            String[] arr = line.split(",");
            return new Score(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Score>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp())
                        .withIdleness(Duration.ofMillis(2000)) // 防止上游某些分区的水位线不推进导致下游的窗口一直不触发（这个分区很久都没数据）
        ).setParallelism(2);


        KeyedStream<Score, String> keyedStream = stream1.keyBy(s -> s.getGender());

        keyedStream
                .window(TumblingEventTimeWindows.of(Time.milliseconds(3000)))
                .apply(new WindowFunction<Score, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Score> input, Collector<String> out) throws Exception {
                        double sum = 0;
                        for (Score score : input) {
                            sum += score.getScore();
                        }
                        out.collect(window.getStart() + " ~ " + window.getEnd() + " : " + sum);
                    }
                }).print();

        env.execute();
    }
}
