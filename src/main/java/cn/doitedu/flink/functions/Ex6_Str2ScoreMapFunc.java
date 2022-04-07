package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import cn.doitedu.flink.pojos.Score;

public class Ex6_Str2ScoreMapFunc implements MapFunction<String, Score> {
    @Override
    public Score map(String value) throws Exception {
        String[] arr = value.split(",");
        return new Score(Integer.parseInt(arr[0]),arr[1],Double.parseDouble(arr[2]),Long.parseLong(arr[3]));
    }
}
