package cn.doitedu.flink.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _02_WordCountStream_Java {
    public static void main(String[] args) throws Exception {

        // 流式计算的入口环境构建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置运行模式为批处理，则虽然用的是流式处理api，但是依然会以批计算模型运行
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> streamSource = env.readTextFile("data/wc/wc.txt");

        streamSource.flatMap(new SplitWordFlatMapFunction())
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        // 流式处理的模式下，需要env.execute() 来触发job执行
        env.execute();

    }
}


class SplitWordFlatMapFunction implements FlatMapFunction<String,Tuple2<String,Integer>>{
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word : line.split(" ")) {
            out.collect(Tuple2.of(word,1));
        }
    }
}