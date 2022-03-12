package cn.doitedu.flink.demos;

import cn.doitedu.flink.functions.MyStateMapFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _14_FlinkStateUes2 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("execution.savepoint.path","D:\\checkpoint\\f99c06b3a25e5bbd08205e32fc5d937b\\chk-106");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 设置状态管理后端组件
        env.setStateBackend(new HashMapStateBackend());

        // 开启和设置checkpoint参数
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 开启task失败后的自动failover策略
        // env.setRestartStrategy(RestartStrategies.noRestart());  // 默认的task级别failover策略，不重启不恢复！
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.milliseconds(2000)));

        // 读socket数据
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        // map数据： 输出“ 本次收到的字符串拼接前2条字符串
        source.map(new MyStateMapFunc()).setParallelism(1).print();

        env.execute();
    }

}
