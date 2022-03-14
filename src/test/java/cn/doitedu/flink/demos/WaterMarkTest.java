package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/9
 **/
public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 9099)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element);
                    }
                }));

        SingleOutputStreamOperator<Long> s1 = source.map(Long::parseLong)
                .setParallelism(2)
                .disableChaining();

        SingleOutputStreamOperator<Long> s2 = s1.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {

                System.out.println("水位线 -- " + ctx.timerService().currentWatermark());
                out.collect(value * 1000);
            }
        }).setParallelism(1).disableChaining();
        s2.print().setParallelism(1);

        /*s2.process(new ProcessFunction<Long, String>() {
            @Override
            public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("--p2-----------------------------");
                System.out.println("ctx.timeStamp -- " + ctx.timestamp());
                System.out.println("ctx.timerService.currentwm -- " + ctx.timerService().currentWatermark());
                out.collect(value + " -->");
                System.out.println("**p2*****************************");
            }
        }).setParallelism(1).print().setParallelism(1);*/


        env.execute();


    }
}
