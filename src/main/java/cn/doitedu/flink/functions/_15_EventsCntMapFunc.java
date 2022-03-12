package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import pojos.EventLog;

public class _15_EventsCntMapFunc extends RichMapFunction<EventLog,String> {

    MapState<String, Integer> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("cnt", String.class, Integer.class));
    }

    @Override
    public String map(EventLog eventLog) throws Exception {

        String eventId = eventLog.getEventId();
        // 更新状态
        if(state.contains(eventId)){
            state.put(eventId, state.get(eventId)+1);
        }else{
            state.put(eventId,1);
        }

        return eventLog.getChannel() + "," + eventId + " : " + state.get(eventId);
    }
}
