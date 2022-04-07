package cn.doitedu.flink.exercise;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.AccessLog;

import java.util.HashSet;

public class Window练习1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 1. 添加涛哥定义的那个随机数据生成器为数据源
        DataStreamSource<String> stream = env.addSource(new AccessLogSource());

        // 2. 将原始的json数据，转成pojo数据
        SingleOutputStreamOperator<AccessLog> stream2 = stream.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String value) throws Exception {
                return JSON.parseObject(value,AccessLog.class);
            }
        });

        // 3. 每2秒求一次最近5秒钟的uv数，pv数，来源渠道数，事件类型数
        stream2
                .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(5000), Time.milliseconds(2000)))
                .apply(new AllWindowFunction<AccessLog, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<AccessLog> values, Collector<String> out) throws Exception {
                        int pvCnt = 0;
                        HashSet<Long> guidSet = new HashSet<>();
                        HashSet<String> channelSet = new HashSet<>();
                        HashSet<String> eventIdSet = new HashSet<>();

                        for (AccessLog value : values) {
                            guidSet.add(value.getGuid());
                            channelSet.add(value.getChannel());
                            eventIdSet.add(value.getEventid());
                            pvCnt++;
                        }
                        out.collect(String.format("uv:%d , pv: %d ,channels: %d , events: %d ", guidSet.size(), pvCnt, channelSet.size(), eventIdSet.size()));
                    }
                }).print();


        env.execute();
    }
}

class AccessLogSource implements SourceFunction<String> {

    String[] channels = {"app", "wx", "h5"};

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (true) {
            int guid = RandomUtils.nextInt(1, 20);
            String eventid = RandomStringUtils.random(1, "abcdefg");
            long timestamp = System.currentTimeMillis();
            String channel = channels[RandomUtils.nextInt(0, 3)];

            ctx.collect(String.format("{\"guid\":%d,\"eventid\":\"%s\",\"timestamp\":%d,\"channel\":\"%s\"}", guid, eventid, timestamp, channel));
            Thread.sleep(RandomUtils.nextInt(500, 2000));
        }
    }

    @Override
    public void cancel() {

    }

}
