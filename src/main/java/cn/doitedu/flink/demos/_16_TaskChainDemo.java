package cn.doitedu.flink.demos;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 算子并行度设置演示
 * 算子chain机制演示
 * 打断默认chain方式演示
 *
 */
public class _16_TaskChainDemo {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //env.disableOperatorChaining(); // 全局禁用算子链 ，那么每一个算子都会成为一个独立的task


        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> source = env.socketTextStream("doit01", 9099).name("source");

        DataStream<String> map1 = source.map(s -> s).name("map1").setParallelism(2).rescale();

        SingleOutputStreamOperator<String> filter1 = map1.filter(s -> !s.startsWith("x")).name("filter1").setParallelism(3);

        SingleOutputStreamOperator<String> windowApply = filter1
                .keyBy(s -> s)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .apply(new WindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {
                        for (String s1 : input) {
                            out.collect(s1);
                        }
                    }
                }).name("window-apply").setParallelism(2);

        DataStream<String> map2 = windowApply.map(s -> s.toUpperCase()).setParallelism(2)/*.slotSharingGroup("x01")*//*.startNewChain()*//*.disableChaining()*/.name("map2").global();

        map2.print().setParallelism(1).name("print").disableChaining();

        env.execute();


    }
}
