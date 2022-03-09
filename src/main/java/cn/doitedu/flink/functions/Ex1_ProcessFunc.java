package cn.doitedu.flink.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojos.EventLog;

public class Ex1_ProcessFunc extends KeyedProcessFunction<String, EventLog, Long> {
    long cnt = 0;
    @Override
    public void processElement(EventLog value, KeyedProcessFunction<String, EventLog, Long>.Context ctx, Collector<Long> out) throws Exception {
        cnt++;
        out.collect(cnt);
    }
}
