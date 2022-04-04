package eagle.etl;

import com.alibaba.fastjson.JSON;
import eagle.pojo.EventBean;
import eagle.pojo.TrafficAnalyseBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/4
 **/
public class ApplogTrafficAnalyse {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造一个kafka的source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("dwd-applog-detail")
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // 从kafka的dwd明细topic读取数据
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "dwd-applog");

        SingleOutputStreamOperator<EventBean> beanStream = sourceStream.map(json -> JSON.parseObject(json, EventBean.class));

        // guid,  访问总时长,  渠道,  终端， 省市区 ，页面， 页面访问时长
        beanStream.keyBy(bean -> bean.getGuid())
                .process(new KeyedProcessFunction<Long, EventBean, TrafficAnalyseBean>() {

                    MapState<String, Long> sessionTimeLongState;
                    ValueState<Long> lastStartTimeState;
                    ValueState<EventBean> eventBeanState;
                    ValueState<Tuple2<String, Long>> pageTimeState;
                    ValueState<Long> lastTimerTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // 开辟一个记录会话及会话时长的map结构状态
                        sessionTimeLongState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("ses_tl", String.class, Long.class));

                        // 开辟一个记录最近一次启动、唤醒时间戳的状态
                        lastStartTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastStartTime", Long.class));

                        // 开辟一个记录当前页面id和加载时间的状态
                        pageTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, Long>>("pageTime", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})));

                        // 开辟一个记录最后一次行为事件EventBean的状态
                        eventBeanState = getRuntimeContext().getState(new ValueStateDescriptor<EventBean>("pageTime", EventBean.class));

                    }

                    @Override
                    public void processElement(EventBean eventBean, Context context, Collector<TrafficAnalyseBean> collector) throws Exception {

                        eventBeanState.update(eventBean);

                        // 判断，当前事件是否是一个app启动事件
                        if ("applaunch".equals(eventBean.getEventid())) {
                            // 更新掉最近启动事件戳状态
                            lastStartTimeState.update(eventBean.getTimestamp());

                            // 清空会话时长状态，添加新的会话
                            sessionTimeLongState.clear();
                            sessionTimeLongState.put(eventBean.getSessionid(),0L);

                            // 注册定时器，用于 按固定频率，每分钟去输出结果
                            long timerTime = eventBean.getTimestamp() + 60 * 1000;
                            lastTimerTime.update(timerTime);  // 将定时器触发时间记录在状态中
                            context.timerService().registerProcessingTimeTimer(timerTime);

                        }

                        // 判断，当前事件是否是一个app唤醒事件
                        if ("wakeup".equals(eventBean.getEventid())) {
                            // 更新掉最近启动事件戳状态
                            lastStartTimeState.update(eventBean.getTimestamp());


                            // 注册定时器，用于 按固定频率，每分钟去输出结果
                            long timerTime = eventBean.getTimestamp() + 60 * 1000;
                            lastTimerTime.update(timerTime);  // 将定时器触发时间记录在状态中
                            context.timerService().registerProcessingTimeTimer(timerTime);

                        }

                        // 判断，当前事件是否是一个app放入后台事件
                        if ("backoff".equals(eventBean.getEventid())) {
                            // 主动输出一次结果
                            Tuple2<String, Long> pageTime = pageTimeState.value();
                            collector.collect(new TrafficAnalyseBean(
                                    eventBean.getGuid(),
                                    eventBean.getSessionid(),
                                    sessionTimeLongState.get(eventBean.getSessionid()),
                                    eventBean.getReleasechannel(),
                                    eventBean.getDevicetype(),
                                    eventBean.getProvince(),
                                    eventBean.getCity(),
                                    eventBean.getRegion(),
                                    pageTime.f0,
                                    eventBean.getTimestamp() - pageTime.f1,
                                    pageTime.f1
                            ));

                            //  停止定时器的结果的输出
                            context.timerService().deleteProcessingTimeTimer(lastTimerTime.value());

                        }

                        // 判断，当前事件是否是一个app关闭事件
                        if ("appclose".equals(eventBean.getEventid())) {
                            // 输出一次会话的结果
                            // 先取到最后一个页面的信息
                            Tuple2<String, Long> pageTime = pageTimeState.value();
                            collector.collect(new TrafficAnalyseBean(
                                    eventBean.getGuid(),
                                    eventBean.getSessionid(),
                                    sessionTimeLongState.get(eventBean.getSessionid()),
                                    eventBean.getReleasechannel(),
                                    eventBean.getDevicetype(),
                                    eventBean.getProvince(),
                                    eventBean.getCity(),
                                    eventBean.getRegion(),
                                    pageTime.f0,
                                    eventBean.getTimestamp() - pageTime.f1,
                                    pageTime.f1
                            ));
                            //  停止定时器的结果的输出
                            context.timerService().deleteProcessingTimeTimer(lastTimerTime.value());

                            // 清除所有的状态
                            sessionTimeLongState.clear();
                            lastStartTimeState.clear();
                            eventBeanState.clear();
                            pageTimeState.clear();
                            lastTimerTime.clear();
                        }

                        // 如果是一个页面加载事件，则往状态中记录本次新打开的页面的id和加载时间
                        if("pageload".equals(eventBean.getEventid())){
                            // 既然新页面加载了，那就可以输出一条结果了，把上一个页面的数据输出
                            Tuple2<String, Long> pageTime = pageTimeState.value();
                            collector.collect(new TrafficAnalyseBean(
                                    eventBean.getGuid(),
                                    eventBean.getSessionid(),
                                    sessionTimeLongState.get(eventBean.getSessionid()),
                                    eventBean.getReleasechannel(),
                                    eventBean.getDevicetype(),
                                    eventBean.getProvince(),
                                    eventBean.getCity(),
                                    eventBean.getRegion(),
                                    pageTime.f0,
                                    eventBean.getTimestamp() - pageTime.f1,
                                    pageTime.f1
                            ));
                            // 然后把状态清空，记录新页面
                            pageTimeState.update(Tuple2.of(eventBean.getProperties().get("pageId"), eventBean.getTimestamp()));
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TrafficAnalyseBean> collector) throws Exception {

                        EventBean eventBean = eventBeanState.value();

                        // 定时器，每分钟触发一次输出当前的结果
                        Tuple2<String, Long> pageTime = pageTimeState.value();
                        collector.collect(new TrafficAnalyseBean(
                                eventBean.getGuid(),
                                eventBean.getSessionid(),
                                sessionTimeLongState.get(eventBean.getSessionid()),
                                eventBean.getReleasechannel(),
                                eventBean.getDevicetype(),
                                eventBean.getProvince(),
                                eventBean.getCity(),
                                eventBean.getRegion(),
                                pageTime.f0,
                                eventBean.getTimestamp() - pageTime.f1,
                                pageTime.f1
                        ));

                        // 定时器在注册时指定的时间到达时，触发，但触发后，就没了
                        // 如果需要在下一个1分钟继续触发，那就需要重新再注册
                        long timerTime = lastTimerTime.value()+60*1000;
                        ctx.timerService().registerEventTimeTimer(timerTime);
                    }
                });


        env.execute();

    }
}
