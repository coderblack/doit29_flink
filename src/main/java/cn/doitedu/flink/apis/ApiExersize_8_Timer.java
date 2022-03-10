package cn.doitedu.flink.apis;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 定时器使用演示
 */
public class ApiExersize_8_Timer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1,a
        // 1,b
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9099);

        SingleOutputStreamOperator<Tuple2<String, String>> s2 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        KeyedStream<Tuple2<String, String>, String> keyed = s2.keyBy(tp -> tp.f0);

        keyed.process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {


            @Override
            public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                if(value.f1.equals("x")){
                    long processingTime = ctx.timerService().currentProcessingTime();

                    // out.collect(String.format("劲爆！警报！用户： %s 做了X",value.f0));
                    // 注册一个定时器（定时任务）
                    System.out.println("监测到用户"+value.f0+"发生了X行为,当前时间为：" + processingTime);
                    ctx.timerService().registerProcessingTimeTimer(  processingTime + 5000);  // 定时器的触发时间，是一个绝对时间
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, String>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(String.format("劲爆！警报！用户： %s 做了X ,告警时间： %d", ctx.getCurrentKey(),timestamp));
            }
        }).setParallelism(2).print();

        env.execute();
    }

}
