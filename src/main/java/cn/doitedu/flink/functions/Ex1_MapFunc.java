package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import pojos.EventLog;

public class Ex1_MapFunc implements MapFunction<String, EventLog> {

    @Override
    public EventLog map(String value) throws Exception {
        String[] arr = value.split(",");

        return new EventLog(Long.parseLong(arr[0]),arr[1],arr[2],Long.parseLong(arr[3]),Long.parseLong(arr[4]));
    }
}
