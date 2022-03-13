package cn.doitedu.flink.apis;

import cn.doitedu.flink.functions.Ex1_FlatMap1;
import cn.doitedu.flink.functions.Ex1_MapFunc;
import cn.doitedu.flink.functions.Ex3_WindowFunc1;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojos.EventLog;

import java.time.Duration;

/**
 * 事件时间语义及其窗口聚合
 */
public class ApiExercise_03_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1,addCart,app,1646784001000,100
        // 1,addCart,app,1646784002000,100
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9099);
        /**
         * 构造一个水印生成策略，带时间戳提取器
         */
        WatermarkStrategy<String> strategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(2000))  // 指定水印生成策略： 容忍乱序数据的周期性生成水印的策略
                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[3]));// 指定时间戳提取器

        // 将原始数据打散成一条一条的日志
        // 1,addCart,14763473469458,app,12387_1,pageView,14763473488458,app,3387
        SingleOutputStreamOperator<String> stream2 = stream1.flatMap(new Ex1_FlatMap1())
                .assignTimestampsAndWatermarks(strategy);  //  给数据流分配一个时间戳和水印生成策略，以让flink为这个流生成水印

        // 把字符串转成EventLog对象
        SingleOutputStreamOperator<EventLog> stream3 = stream2.map(new Ex1_MapFunc());


        // 开滚动窗口进行窗口聚合计算
        stream3.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(7000)))
                /*.apply(new Ex3_WindowFunc1())
                .print()*/;

        OutputTag<EventLog> lateTag = new OutputTag<>("late", TypeInformation.of(EventLog.class));  // 侧输出流标签

        // 开滑动窗口进行窗口聚合计算
        SingleOutputStreamOperator<String> stream4 =
                stream3.windowAll(SlidingEventTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(5000)))
                .allowedLateness(Time.milliseconds(2000))   // 允许迟到2秒; 默认是0
                .sideOutputLateData(lateTag)   // 超过迟到最大允许时间的数据，收集到侧输出流
                .apply(new Ex3_WindowFunc1());

        stream4.print();

        // 获取侧流，做一些自己的补救逻辑
        stream4.getSideOutput(lateTag).print();


        env.execute();
    }
}
