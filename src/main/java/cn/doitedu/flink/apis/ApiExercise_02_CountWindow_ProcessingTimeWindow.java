package cn.doitedu.flink.apis;

import cn.doitedu.flink.functions.Ex1_FlatMap1;
import cn.doitedu.flink.functions.Ex1_MapFunc;
import cn.doitedu.flink.functions.Ex1_WindowFunc1;
import cn.doitedu.flink.trigger.MyCountTrigger;
import cn.doitedu.flink.trigger.MyEvictor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import cn.doitedu.flink.pojos.EventLog;

/**
 * 窗口api
 *   计数窗口
 *   processingTime语义的时间窗口
 */
public class ApiExercise_02_CountWindow_ProcessingTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造一个kafka的数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("logs")
                .setGroupId("ex01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .build();

        // 添加数据源
        DataStreamSource<String> stream1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");

        // 将原始数据打散成一条一条的日志
        // 1,addCart,14763473469458,app,12387_1,pageView,14763473488458,app,3387
        SingleOutputStreamOperator<String> stream2 = stream1.flatMap(new Ex1_FlatMap1());

        // 把字符串转成EventLog对象
        SingleOutputStreamOperator<EventLog> stream3 = stream2.map(new Ex1_MapFunc());

        // 每隔5条数据计算一次最近5条数据中，事件停留时长总和 --> 滚动窗口，size=5， step=5
        stream3.countWindowAll(5)
                .sum("stayLong")
        /*.print()*/;
        // EventLog(guid=1, eventId=addcart, channel=app, timeStamp=17348346, stayLong=5000)

        // 每隔2条数据计算一次最近5条数据中，事件停留时长总和 --> 滑动窗口，size=5， step=2
        // 便利api创建计数窗口
        stream3.countWindowAll(5, 2)
                .sum("stayLong")
        /*.print()*/;


        // 通用的开窗的方法
        stream3.windowAll(GlobalWindows.create())  // 先定义一个抽象的全局窗口
                .trigger(CountTrigger.of(2))  // 为窗口添加触发计算的条件：  每来2条数据触发一次窗口计算
                .evictor(CountEvictor.of(5))  // 为窗口添加计算前、后移除元素的逻辑：每次计算前，移除掉窗口中多余（只保留5条）的早期元素
                .sum("stayLong")
        /*.print()*/;

        /**
         * 通过自定义Trigger和Evictor来实现一个自定义的计数滑动窗口
         * Trigger：窗口触发器，相当于指定 窗口的 结束边界
         * Evictor：元素移除器，多用于指定 窗口的 起始边界
         */
         stream3.windowAll(GlobalWindows.create())
                .trigger(new MyCountTrigger())
                .evictor(new MyEvictor())
                .sum("stayLong")
               .print();

        /**
         * 滚动、滑动，时间窗口
         * 时间语义：processing Time
         */
        stream3.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(5000))); // 滚动窗口
        stream3.windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(5000))) // 滑动窗口
                /*.sum("stayLong")*/
                .apply(new Ex1_WindowFunc1())
                .print();

        env.execute();
    }

}
