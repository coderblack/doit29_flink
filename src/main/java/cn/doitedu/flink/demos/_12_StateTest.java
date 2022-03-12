package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class _12_StateTest {

    public static void main(String[] args) throws Exception {

        // 本地模拟分布式提交任务时，指定从哪个保存点来恢复job的状态
        Configuration conf = new Configuration();
        /*conf.setString("execution.savepoint.path","D:\\checkpoint\\b9d0b2c2f976d9a0380761f870b0f335\\chk-280");*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 开启状态检查点机制(它将会定期对整个系统中各个task的状态进行快照持久化，以便失败重启后还能从失败之前的状态恢复）
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        // checkpoint机制触发后，持久化保存各task状态数据的存储位置（生产中用hdfs）
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint/");
        // 开启自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.milliseconds(2000)));

        // 指定状态后端存储（内存）
        env.setStateBackend(new HashMapStateBackend());
        // 指定状态后端存储（RocksDB）
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // 1
        // 2
        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9099);

        /**
         * 自己管理状态
         */

        // 累加求和
        /*s1.map(new MapFunction<String, Integer>() {
            int sum = 0;
            @Override
            public Integer map(String value) throws Exception {
                sum += Integer.parseInt(value);
                return sum;
            }cxx
        }).setParallelism(1).print().setParallelism(1);*/

        // 每5个元素拼接字符串输出
        s1.map(new MapFunction<String, String>() {
            // 初始化状态
            MyState myState = new MyState();
            @Override
            public String map(String value) throws Exception {
                // 更新状态
                myState.setCnt(myState.getCnt()+1);
                myState.setStr(myState.getStr()+value);

                if(myState.getCnt() == 5) {

                    String res = myState.getStr();
                    // 清空状态
                    myState.setCnt(0);
                    myState.setStr("");

                    return res;
                }
                return "";
            }
        }).setParallelism(1).print();


        /**
         * 让flink接管状态
         */
        s1.map(s -> {
                    if(s.equals("99")) throw new Exception("我就是要故意抛个异常.....");
                    return Integer.parseInt(s);
                })
                .keyBy(s -> 1)
                .sum("*")
                .print();

        env.execute();
    }

}

class MyState implements Serializable {
    private String str = "";
    private int cnt = 0;

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public MyState() {
    }

    public MyState(String str, int cnt) {
        this.str = str;
        this.cnt = cnt;
    }
}