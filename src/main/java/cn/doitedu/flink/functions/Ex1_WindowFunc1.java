package cn.doitedu.flink.functions;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.EventLog;

public class Ex1_WindowFunc1  implements AllWindowFunction<EventLog, String, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<EventLog> values, Collector<String> out) throws Exception {
        long sum = 0;

        for (EventLog value : values) {
            sum += value.getStayLong();
        }
        out.collect(window.getStart() + "," + window.getEnd() + "," + window.maxTimestamp() + " : " + sum);
    }
}
