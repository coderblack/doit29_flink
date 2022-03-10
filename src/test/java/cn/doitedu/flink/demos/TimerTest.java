package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class TimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9099);
        env.getConfig().setAutoWatermarkInterval(10);

        SingleOutputStreamOperator<String> stream = s1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[1])));


        stream.keyBy(s->1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                        if(value.contains("x")) {
                            long processingTime = ctx.timerService().currentProcessingTime();
                            long watermark = ctx.timerService().currentWatermark();
                            System.out.println("当前数据到达处理时间： " + processingTime +", ctx.timestamp : " + ctx.timestamp() );
                            System.out.println("当前数据到达事件时间watermark： " + watermark +", ctx.timestamp : " + ctx.timestamp() );
                            //ctx.timerService().registerProcessingTimeTimer(l+5000);
                            ctx.timerService().registerEventTimeTimer(watermark+5000);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        Integer currentKey = ctx.getCurrentKey();
                        out.collect(String.format("定时器信息： %d ,%s" ,timestamp,currentKey));
                    }
                }).print();

        env.execute();

    }
}
