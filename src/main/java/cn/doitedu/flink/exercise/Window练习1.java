package cn.doitedu.flink.exercise;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Window练习1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 1. 添加涛哥定义的那个随机数据生成器为数据源

        // 2. 将原始的json数据，转成pojo数据


        // 3. 每2秒求一次最近5秒钟的uv数，pv数，来源渠道数，事件类型数
        // source.windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(5000),Time.milliseconds(2000)))



        env.execute();
    }
}

class AccessLogSource implements SourceFunction<String>{

    String[] channels = {"app","wx","h5"};
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(true){
            int guid = RandomUtils.nextInt(1, 20);
            String eventid = RandomStringUtils.random(1, "abcdefg");
            long timestamp = System.currentTimeMillis();
            String channel =channels[RandomUtils.nextInt(0,3)];

            ctx.collect(String.format("{\"guid\":%d,\"eventid\":\"%s\",\"timestamp\":%d,\"channel\":\"%s\"}",guid,eventid,timestamp,channel));
            Thread.sleep(RandomUtils.nextInt(500,2000));
        }
    }

    @Override
    public void cancel() {

    }

}
