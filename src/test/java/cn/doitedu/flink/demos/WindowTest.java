package cn.doitedu.flink.demos;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pojos.AccessLog;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/8
 **/
public class WindowTest {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);
        SingleOutputStreamOperator<AccessLog> logs = source.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AccessLog(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<AccessLog>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        .withTimestampAssigner((AccessLog element, long recordTimestamp) -> element.getTimestamp())
                        /*.withIdleness(Duration.ofMillis(1000))*/
        );

        OutputTag<AccessLog> lateTag = new OutputTag<>("late", Types.POJO(AccessLog.class));
        SingleOutputStreamOperator<String> res = logs.keyBy(s -> s.getChannel())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5000))).allowedLateness(Time.milliseconds(1000))
                .sideOutputLateData(lateTag)
                .aggregate(new AggregateFunction<AccessLog, HashMap<String, Integer>, HashMap<String, Integer>>() {
                               @Override
                               public HashMap<String, Integer> createAccumulator() {
                                   return new HashMap<String, Integer>();
                               }

                               @Override
                               public HashMap<String, Integer> add(AccessLog value, HashMap<String, Integer> accumulator) {
                                   accumulator.put(value.getEventid(), accumulator.getOrDefault(value.getEventid(), 0) + 1);
                                   return accumulator;
                               }

                               @Override
                               public HashMap<String, Integer> getResult(HashMap<String, Integer> accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {

                                   for (Map.Entry<String, Integer> entry : a.entrySet()) {
                                       b.put(entry.getKey(), b.getOrDefault(entry.getKey(), 0) + entry.getValue());
                                   }
                                   return b;
                               }
                           },
                        new WindowFunction<HashMap<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<HashMap<String, Integer>> input, Collector<String> out) throws Exception {
                                out.collect(s + " : " + input.iterator().next());
                                System.out.println(window.getStart() + "," + window.getEnd() + "," + window.maxTimestamp());
                            }
                        }
                        /*,new ProcessWindowFunction<HashMap<String, Integer>, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<HashMap<String, Integer>> elements, Collector<String> out) throws Exception {
                                out.collect(s + " : " + elements.iterator().next());
                            }
                        }*/
                )
                ;

        res.print();
        res.getSideOutput(lateTag).print();

        env.execute();
    }
}

class MyWaterMarkGeneratorSupplier implements WatermarkGeneratorSupplier<AccessLog>, SerializableTimestampAssigner<AccessLog> {


    @Override
    public WatermarkGenerator<AccessLog> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

        return new MyWaterMarkGenerator();
    }

    @Override
    public long extractTimestamp(AccessLog element, long recordTimestamp) {
        return element.getTimestamp();
    }
}

class MyWaterMarkGenerator implements WatermarkGenerator<AccessLog> {

    long maxTimestamp;

    @Override
    public void onEvent(AccessLog event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = eventTimestamp;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - 1));

    }
}

class MyTimeStampAssigner implements SerializableTimestampAssigner<AccessLog> {

    @Override
    public long extractTimestamp(AccessLog element, long recordTimestamp) {
        return element.getTimestamp();
    }
}


