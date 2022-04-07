package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import cn.doitedu.flink.pojos.EventLog;

public class Ex1_ReduceFunc implements ReduceFunction<EventLog> {
    @Override
    public EventLog reduce(EventLog value1, EventLog value2) throws Exception {
        value1.setStayLong(value1.getStayLong()+ value2.getStayLong());
        return value1;
    }
}
