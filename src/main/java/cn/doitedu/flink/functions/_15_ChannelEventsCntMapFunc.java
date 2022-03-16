package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import pojos.EventLog;

public class _15_ChannelEventsCntMapFunc extends RichMapFunction<EventLog, String> {

    ValueState<Integer> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(2000)).cleanupFullSnapshot().neverReturnExpired().useProcessingTime().updateTtlOnReadAndWrite().build();
        ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("cnt", Integer.class);
        desc.enableTimeToLive(ttlConfig);
        // 获取单值状态管理器
        valueState = getRuntimeContext().getState(desc);
    }

    @Override
    public String map(EventLog eventLog) throws Exception {

        // 来一条数据，就对状态更新
        valueState.update((valueState.value() == null ? 0 : valueState.value()) + 1);

        return eventLog.getChannel() + " : " + valueState.value();
    }

}
