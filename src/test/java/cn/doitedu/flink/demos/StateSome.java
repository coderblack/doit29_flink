package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.io.*;
import java.util.Iterator;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/8
 **/
public class StateSome {

    public static void main(String[] args) throws Exception {

        // stateLoss();
        // stateLoss2();
        // stateSelf();
        stateFlink();

    }

    public static void stateLoss() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.socketTextStream("localhost", 9099).map(s -> Integer.parseInt(s));

        source.keyBy(s -> 1).sum(0).print();
        env.execute();
    }

    public static void stateLoss2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.socketTextStream("localhost", 9099).map(s -> Integer.parseInt(s));

        source.keyBy(s -> 1).process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            int sum = 0;

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                sum += value;
                out.collect(sum);
            }
        }).print();
        env.execute();
    }

    public static void stateSelf() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.socketTextStream("localhost", 9099).map(s -> Integer.parseInt(s));

        source.keyBy(s -> 1).process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            int sum = 0;
            BufferedReader bw = null;

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                try {
                    bw = new BufferedReader(new FileReader("./state"));
                    sum = Integer.parseInt(bw.readLine());
                } catch (Exception e) {
                    sum = 0;
                }
                sum += value;
                out.collect(sum);

                BufferedWriter bw = new BufferedWriter(new FileWriter("./state"));
                bw.write(sum + "");
                bw.close();
            }
        }).print();
        env.execute();
    }

    public static void stateFlink() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "file:///d:/ckpt/68be02e1fa8d401082f9c048be021a38/chk-8");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Integer> source = env.socketTextStream("localhost", 9099).map(s -> Integer.parseInt(s));
        source.map(new MyMap())
                .keyBy(s -> s).process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            ValueState<Integer> sum;
            @Override
            public void open(Configuration parameters) throws Exception {
                sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
            }

            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (sum.value() == null) {
                    sum.update(0);
                }
                sum.update(sum.value() + value);
                out.collect(sum.value());
            }
        }).setParallelism(2).print();
        env.execute();
    }

}

class MyMap implements MapFunction<Integer, Integer>, CheckpointedFunction {
    ListState<Integer> x;

    @Override
    public Integer map(Integer value) throws Exception {
        System.out.println(x.get().iterator().next());
        return value;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        x.add(1000);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        x = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("x", Integer.class));

        Iterator<Integer> iter = x.get().iterator();
        while (iter.hasNext()) {
            System.out.println(context.getRestoredCheckpointId() + "-----" + iter.next());
        }
    }
}