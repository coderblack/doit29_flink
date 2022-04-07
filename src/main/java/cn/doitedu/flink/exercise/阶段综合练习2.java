package cn.doitedu.flink.exercise;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.EventLog;

import java.time.Duration;
import java.util.HashSet;

/**
 * 先写一个数据模拟器，不断生成如下数据,并写入kafka
 * {"guid":1,"eventId":"addCart","channel":"app","timeStamp":1646784001000,"stayLong":200}
 * {"guid":1,"eventId":"share","channel":"h5","timeStamp":1646784002000,"stayLong":300}
 * {"guid":1,"eventId":"pageView","channel":"wxapp","timeStamp":1646784002000,"stayLong":260}
 * <p>
 * 然后，开发flink程序，从kafka中消费上述数据，做如下处理：
 * 1.过滤掉所有来自于H5渠道的数据
 * <p>
 * 2.每5秒钟统计一次最近5秒钟的每个渠道上的行为人数
 * <p>
 * 3.每5秒钟算一次最近30秒的如下规则：
 * 一个人在最近30秒内addcart事件超过5次，则输出一个告警信息
 * 一个人在最近30秒内，如果出现某次事件的行为时长>300，则输出一个告警信息
 * 一个人如果发生了事件addCart事件后的5秒内没有做 payment事件，则输出一个“催支付”的信息
 */
public class 阶段综合练习2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setGroupId("gpx")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("tpc0312")
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<EventLog> strategy = WatermarkStrategy.<EventLog>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<EventLog>() {
                    @Override
                    public long extractTimestamp(EventLog element, long recordTimestamp) {
                        return element.getTimeStamp();
                    }
                })
                .withIdleness(Duration.ofMillis(1000));


        SingleOutputStreamOperator<EventLog> stream1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk")
                .map(new MapFunction<String, EventLog>() {
                    @Override
                    public EventLog map(String value) throws Exception {
                        return JSON.parseObject(value, EventLog.class);
                    }
                })
                .filter(new FilterFunction<EventLog>() {
                    @Override
                    public boolean filter(EventLog value) throws Exception {
                        return !value.getChannel().equals("h5");
                    }
                })
                .assignTimestampsAndWatermarks(strategy);

        // 按渠道keyBy
        KeyedStream<EventLog, String> channelKeyed = stream1.keyBy(new KeySelector<EventLog, String>() {
            @Override
            public String getKey(EventLog value) throws Exception {
                return value.getChannel();
            }
        });

        // 每个渠道上，每5秒计算一次最近5秒的人数
        channelKeyed.window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .apply(new WindowFunction<EventLog, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<EventLog> elements, Collector<String> out) throws Exception {
                        HashSet<Long> set = new HashSet<>();

                        for (EventLog element : elements) {
                            set.add(element.getGuid());
                        }
                        out.collect(String.format("渠道: %s , 人数: %d", key, set.size()));
                    }
                })/*.print()*/;

        // 按guid 来keyBy
        KeyedStream<EventLog, Long> guidKeyed = stream1.keyBy(new KeySelector<EventLog, Long>() {
            @Override
            public Long getKey(EventLog value) throws Exception {
                return value.getGuid();
            }
        });

        // 每5秒钟算一次
        // * 一个人在最近30秒内addcart事件超过5次，则输出一个告警信息
        // * 一个人在最近30秒内，如果出现某次事件的行为时长>300，则输出一个告警信息
        guidKeyed.window(SlidingEventTimeWindows.of(Time.milliseconds(30000), Time.milliseconds(5000)))
                .apply(new WindowFunction<EventLog, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<EventLog> elements, Collector<String> out) throws Exception {
                        int count = 0;

                        for (EventLog element : elements) {
                            if (element.getEventId().equals("addcart")) count++;
                            if (element.getStayLong() > 300)
                                out.collect(String.format("告警：事件时长超标： 用户： %d , 事件id： %s , 事件时长：%d ", key, element.getEventId(), element.getStayLong()));
                        }

                        if (count > 5)
                            out.collect(String.format("告警：addcart次数超标： 用户： %d ,  addcart次数： %d", key, count));
                    }
                })/*.print()*/;


        // 一个人如果发生了事件addCart事件后的5秒内没有做 payment事件，则输出一个“催支付”的信息
        guidKeyed
                .process(new KeyedProcessFunction<Long, EventLog, String>() {
                    ListState<String> state;
                    boolean flag = false;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getListState(new ListStateDescriptor<String>("stat", String.class));
                    }

                    @Override
                    public void processElement(EventLog eventLog, KeyedProcessFunction<Long, EventLog, String>.Context ctx, Collector<String> out) throws Exception {
                        if (flag) state.add(eventLog.getEventId());

                        if (eventLog.getEventId().equals("addcart")) {
                            // 注册定时器
                            ctx.timerService().registerEventTimeTimer(eventLog.getTimeStamp() + 5000);
                            // 把标记设置为true
                            flag = true;
                        }
                    }

                    /**
                     * 定时器触发时调用的方法
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, EventLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 判断这个人在这5秒的计时时段内是否做过payment
                        Iterable<String> events = state.get();

                        boolean isPayed = false;
                        for (String event : events) {
                            if (event.equals("payment")) {
                                isPayed = true;
                                break;
                            }
                        }

                        if(!isPayed) {
                            out.collect(String.format("催支付短信： 用户： %d", ctx.getCurrentKey()));
                        }
                        // 清空状态
                        state.clear();
                        flag = false;

                    }
                }).print();


        env.execute();
    }

}
