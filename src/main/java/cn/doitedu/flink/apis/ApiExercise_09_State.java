package cn.doitedu.flink.apis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.EventLog;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * 状态相关api大全
 * 算子状态
 * keyed状态
 */
public class ApiExercise_09_State {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果程序中使用了状态，一般都需要去设置checkpoint以便于容错
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage(new Path("hdfs://doit01:8020/flink-jobs-checkpoints/")); // 指定checkpoint数据的存储位置
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10); // 容许checkpoint失败的最大次数
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // checkpoint的算法模式（是否需要对齐）
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // job取消时是否保留checkpoint数据
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMillis(2000));  // 设置checkpoint对齐的超时时间
        env.getCheckpointConfig().setCheckpointInterval(2000); // 两次checkpoint的最小间隔时间，为了防止两次checkpoint的间隔时间太短
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);  // 最大并行的checkpoint数

        // 要用状态，最好还要指定状态后端(默认是HashMapStateBackend)
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // 既然用了状态，并且启用了checkpoint，那么你肯定想让他自动恢复
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        // 此处的MapFunction中使用了算子状态
        SingleOutputStreamOperator<EventLog> stream1 = source.map(new MapFunction<String, EventLog>() {
            @Override
            public EventLog map(String value) throws Exception {
                return null;
            }
        });

        stream1.keyBy(EventLog::getChannel)
                .process(new KeyedProcessFunction<String, EventLog, EventLog>() {

                    ValueState<Integer> valueState1;
                    ValueState<List<EventLog>> valueState2;
                    ListState<String> listState;
                    MapState<String, EventLog> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // 单值状态
                        ValueState<Integer> valueState1 = getRuntimeContext().getState(new ValueStateDescriptor<>("valueState", Integer.class));
                        ValueState<List<EventLog>> valueState2 = getRuntimeContext().getState(new ValueStateDescriptor<>("valueState2", TypeInformation.of(new TypeHint<List<EventLog>>() {
                        })));

                        // list状态
                        ListState<String> listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listState", String.class));

                        // map状态
                        MapState<String, EventLog> mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, EventLog>("mapState", String.class, EventLog.class));

                    }

                    /**
                     * 数据处理逻辑方法
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(EventLog eventLog, KeyedProcessFunction<String, EventLog, EventLog>.Context ctx, Collector<EventLog> out) throws Exception {
                        Integer value1 = valueState1.value();  // 从状态容器中取值
                        List<EventLog> eventLogs = valueState2.value();  // 从状态容器中取值

                        valueState1.update(100);  // 更新状态


                        if(eventLog.getEventId().equals("addCart")){
                            ctx.timerService().registerEventTimeTimer(eventLog.getTimeStamp()+60000);  // 一分钟后触发的定时器
                        }
                        // 取消一个之前注册的定时器的api
                        ctx.timerService().deleteEventTimeTimer(eventLog.getTimeStamp()+60000);


                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, EventLog, EventLog>.OnTimerContext ctx, Collector<EventLog> out) throws Exception {
                        // 定时器触发时需要执行的动作

                        // 在onTimer中也可以取消定时器
                        ctx.timerService().deleteEventTimeTimer(timestamp+60000);

                        // 在onTimer中也可以注册定时器
                        ctx.timerService().registerProcessingTimeTimer(timestamp+80000);

                    }
                });


    }
}

class MyMapFunc extends RichMapFunction<String, EventLog> implements CheckpointedFunction {
    ListState<EventLog> operatorState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public EventLog map(String value) throws Exception {
        String[] split = value.split(",");
        EventLog eventLog = new EventLog(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]), Long.parseLong(split[4]));

        // 可以使用上面的 算子状态
        Iterable<EventLog> eventLogs = operatorState.get();  // 取到状态中数据
        operatorState.update(Arrays.asList(eventLog, eventLog));  // 往状态中放入数据

        return eventLog;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 快照之前需要做的一些状态更新或者清除等操作
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        operatorState.clear();
        operatorState.addAll(Arrays.asList(null));
    }

    /**
     * 状态初始化
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        operatorState = context.getOperatorStateStore().getListState(new ListStateDescriptor<EventLog>("state", EventLog.class));

    }
}


