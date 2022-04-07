package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import cn.doitedu.flink.pojos.EventLog;

import java.util.Iterator;

public class _15_LatestEventsMapFunc extends RichMapFunction<EventLog, String> {
    ListState<String> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getListState(new ListStateDescriptor<String>("state", String.class));
    }

    @Override
    public String map(EventLog eventLog) throws Exception {

        Iterator<String> iterator = state.get().iterator();
        int cnt = 0;
        while (iterator.hasNext()) {
            iterator.next();
            cnt++;
        }

        if (cnt >= 3) {
            iterator = state.get().iterator();
            iterator.hasNext();
            iterator.next();
            iterator.remove();
        }

        state.add(eventLog.getEventId());

        // 循环拼接字符串
        iterator = state.get().iterator();
        StringBuilder sb = new StringBuilder();
        sb.append(eventLog.getChannel()).append(",");
        while (iterator.hasNext()) {
            sb.append(iterator.next()).append(",");
        }

        return sb.toString();
    }
}
