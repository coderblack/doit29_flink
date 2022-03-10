package cn.doitedu.flink.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import pojos.Score;

public class Ex6_AvgScoreWindowFunc implements WindowFunction<Score, String, String, GlobalWindow> {
    // s : 当前的key
    // window 当前的窗口
    // input：当前窗口中的所有数据
    // out: 结果输出器
    @Override
    public void apply(String s, GlobalWindow window, Iterable<Score> input, Collector<String> out) throws Exception {
        int cnt = 0;
        double scoreSum = 0.0;
        for (Score score : input) {
            cnt++;
            scoreSum += score.getScore();
        }
        out.collect(String.format("平均分为:  %.2f", scoreSum / cnt));
    }
}