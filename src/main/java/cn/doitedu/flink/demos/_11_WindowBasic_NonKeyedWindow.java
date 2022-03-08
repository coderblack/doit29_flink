package cn.doitedu.flink.demos;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class _11_WindowBasic_NonKeyedWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //SingleOutputStreamOperator<Long> source = env.socketTextStream("localhost", 9099).map(s -> Long.parseLong(s));

        DataStreamSource<Long> source = env.addSource(new TimeDataSource());

        // 计数窗口-滚动窗口，  窗口长度为：3（条数据）
        SingleOutputStreamOperator<Long> sum = source.countWindowAll(3).sum(0);
        /*sum.print();*/

        // 计数窗口-滑动窗口， 窗口长度为：3， 滑动步长：1 （意味着每来1条数据就会计算一次）
        source.countWindowAll(3, 3).sum(0)/*.print()*/;

        // 在底层，一条数据该划分到哪一个窗口，是用 WindowAssigner决定的
        // 滚动处理时间窗口
        source.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(5000)))
                .apply(new AllWindowFunction<Long, Long, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<Long> out) throws Exception {
                        System.out.println("window start : " + window.getStart());
                        System.out.println("window end : " + window.getEnd());
                        long sum = 0;
                        for (Long value : values) {
                            sum += value;
                        }
                        out.collect(sum);
                    }
                })
                .print();
        env.execute();
    }
}

class TimeDataSource implements SourceFunction<Long>{

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long i = 1;
        while(true){
            System.out.println(System.currentTimeMillis() + " -- " + i);
            ctx.collect(i);
            i++;
            Thread.sleep(800);
        }
    }

    @Override
    public void cancel() {

    }
}
