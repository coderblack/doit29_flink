package cn.doitedu.flink.demos;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/8
 **/
public class FailoverTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "file:///d:/ckpt/036b20f6d0faef7e41eecc498e06ca75/chk-209");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(5000)));

        DataStreamSource<String> source = env.socketTextStream("localhost", 9088);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = source.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], Integer.valueOf(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        ds1.map(new Map1(10, 7))
                .map(new Map1(20,5))
                .print();

        env.execute();
    }
}

class Map1 implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, CheckpointedFunction {
    int id;
    int x;

    public Map1() {
    }

    public Map1(int id, int x) {
        this.id = id;
        this.x = x;
    }

    ListState<Integer> pre_element;

    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
        for (Integer integer : pre_element.get()) {
            System.out.println("map"+id+"状态值: " + integer);
        }
        pre_element.add(value.f1*id);
        if (value.f1 == x) {
            throw new Exception("故意抛异常");
        }
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //System.out.println("map" + id + " -- 状态被执行快照 ---" + System.currentTimeMillis());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        pre_element = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("pre_element", Integer.class));
    }
}