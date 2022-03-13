package cn.doitedu.flink.apis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pojos.Score;

public class ApiExercise_7_ProcessFunctions {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9099);


        /**
         * 针对单元素流，调用process，里面就传 ProcessFunction
         */
        s1.process(new ProcessFunction<String, Score>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Score>.Context ctx, Collector<Score> out) throws Exception {
                TimerService timerService = ctx.timerService();
                Long timestamp = ctx.timestamp(); // 当前的数据的事件时间


                String taskName = getRuntimeContext().getTaskName(); // 可以通过运行时上下文，获取到很多有用的运行时信息

                String[] arr = value.split(",");
                out.collect(new Score(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3])));
            }
        });


        /**
         * key后的流，process里面传 KeyedProcessFunction
         */
        s1.keyBy(s -> 1)
                .process(new KeyedProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) throws Exception {
                        TimerService timerService = ctx.timerService();  //  时间服务
                        Long timestamp = ctx.timestamp();  // 当前数据的事件时间
                        Integer currentKey = ctx.getCurrentKey(); // 可以拿到当前数据所属的 key

                        // 测流输出
                        ctx.output(new OutputTag<String>("侧输出", TypeInformation.of(String.class)),"我的测流输出数据");

                        String taskName = getRuntimeContext().getTaskName();  // 本任务实例的task名称

                        out.collect("主流输出数据");

                    }
                });

        /**
         * 开了AllWindow窗口（计数）
         * process里面传 ProcessAllWindowFunction
         */
        s1.countWindowAll(5)
                .process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, GlobalWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        GlobalWindow window = context.window();  // 可以多拿到一个信息：本条数据所属的窗口的信息
                        context.output(new OutputTag<String>("侧输出", TypeInformation.of(String.class)),"我的测流输出数据");
                    }
                });


        /**
         * 开了AllWindow窗口（时间）
         * process里面传 ProcessAllWindowFunction
         */
        s1.windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(5000),Time.milliseconds(2000)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                    }
                });

        /**
         * 开了KeyBy后的window（计数窗口）
         * process里面传  ProcessWindowFunction
         */
        s1.keyBy(s->1)
                .countWindow(5)
                .process(new ProcessWindowFunction<String, String, Integer, GlobalWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<String, String, Integer, GlobalWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                    }
                });

        /**
         * 开了KeyBy后的window（计数窗口）
         * process里面传  ProcessWindowFunction
         */
        s1.keyBy(s->1)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                .process(new ProcessWindowFunction<String, Object, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<String, Object, Integer, TimeWindow>.Context context, Iterable<String> elements, Collector<Object> out) throws Exception {

                    }
                });





    }
}
