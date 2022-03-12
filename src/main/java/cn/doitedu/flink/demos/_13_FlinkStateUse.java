package cn.doitedu.flink.demos;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;

public class _13_FlinkStateUse {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);

        // 是用于重启一个job时，指定从某个checkpoint数据来恢复状态
        conf.setString("execution.savepoint.path","D:\\checkpoint\\129e9c0230aca31f30408623c0f468be\\chk-300");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setStateBackend(new HashMapStateBackend());  // 默认的状态管理器
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());  // 支持状态数据的增量更新


        // 启用checkpoint
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        // 指定checkpoint的远程存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");
        // job被取消时，是否选择继续保留外部存储的checkpoint数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 快照机制的模式：Exactly_ONCE，AT_LEAST_ONCE，跟状态一致性的保证有关
        // EXACTLY_ONCE : 保证，进入处理系统的每一条数据，对任何一个算子运行实例的状态都只会精确影响一次（底层就是checkpoint算法中会运用对齐算法）
        // AT_LEAST_ONCE: 保证进入处理系统的每一条数据，对任何一个算子运行实例的状态至少影响一次，不承诺仅一次（底层就是checkpoint算法中运用非对齐算法）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<Integer> s1 = env.addSource(new SequenceDigits()).setParallelism(1);
        s1.print();


        env.execute();
    }
}

class SequenceDigits implements SourceFunction<Integer>, CheckpointedFunction {

    ListState<Integer> listState;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        Integer startNumber;

        Iterator<Integer> iterator = listState.get().iterator();
        if (iterator.hasNext()) {
            startNumber = iterator.next();
        } else {
            startNumber = 0;
        }

        while (true) {
            ctx.collect(startNumber);
            startNumber++;

            // 把状态清空
            listState.clear();
            // 更新状态的数据
            listState.add(startNumber);

            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 对状态数据进行快照前要做的事情
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("状态被快照.........");
    }

    /**
     * 初始化状态数据
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("s1_state", Integer.class);
        listState = context.getOperatorStateStore().getListState(descriptor);
    }
}
