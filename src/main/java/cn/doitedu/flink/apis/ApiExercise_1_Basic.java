package cn.doitedu.flink.apis;

import cn.doitedu.flink.functions.Ex1_FlatMap1;
import cn.doitedu.flink.functions.Ex1_MapFunc;
import cn.doitedu.flink.functions.Ex1_ProcessFunc;
import cn.doitedu.flink.functions.Ex1_ReduceFunc;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import pojos.EventLog;

/**
 * 从kafka中获取行为日志数据
 * 做各种变化来练习api
 */
public class ApiExercise_1_Basic {

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

        // 把eventid为空的数据过滤掉
        SingleOutputStreamOperator<EventLog> stream4 = stream3.filter(value -> StringUtils.isNotBlank(value.getEventId()));

        // 各类聚合练习：
        // 全局聚合，在api中不支持，必须先分组

        // 按channel分组
        KeyedStream<EventLog, String> channelKeyed = stream4.keyBy(eventLog -> eventLog.getChannel());

        // 各种渠道的日志条数
        SingleOutputStreamOperator<Long> logCnt = channelKeyed.process(new Ex1_ProcessFunc());

        // 各渠道的事件停留总时长
        SingleOutputStreamOperator<EventLog> stayLong = channelKeyed.sum("stayLong");

        // 与上面的sum的结果一模一样，会多出来  总时长以外的 EventLog中的字段（这些字段取的都是第一条日志的值）
        SingleOutputStreamOperator<EventLog> stayLong2 = channelKeyed.reduce(new Ex1_ReduceFunc());

        // 各渠道的事件的停留时长最大值
        SingleOutputStreamOperator<EventLog> maxStayLong = channelKeyed.max("stayLong");

        // 各渠道的事件的停留时长最小值
        SingleOutputStreamOperator<EventLog> minStayLong = channelKeyed.min("stayLong");

        // 各渠道中停留时长最长的一个事件
        SingleOutputStreamOperator<EventLog> maxStayLongLog = channelKeyed.maxBy("stayLong");

        // 各渠道中停留时长最短的一个事件
        SingleOutputStreamOperator<EventLog> minStayLongLog = channelKeyed.minBy("stayLong");


        stream4.print();

        env.execute();

    }
}
