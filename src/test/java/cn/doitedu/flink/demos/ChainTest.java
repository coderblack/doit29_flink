package cn.doitedu.flink.demos;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ChainTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        SingleOutputStreamOperator<String> source = env.socketTextStream("localhost", 9099)
                .name("source")
                .slotSharingGroup("a");

        DataStream<String> map1 = source.map(s -> s.toUpperCase()).name("map1").setParallelism(2).rescale();
        DataStream<String> map2 = source.map(s -> s.toUpperCase()).name("map1").setParallelism(2).rescale();

        SingleOutputStreamOperator<String> co1 = map1.connect(map2).map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        }).disableChaining().setParallelism(4).slotSharingGroup("other");

        DataStream<String> mp3 = co1.map(s -> s).setParallelism(2).shuffle();

        mp3.map(s->s.toUpperCase()).keyBy(s->s).map(s->s).setParallelism(2).print().setParallelism(2);



        env.execute();


    }
}
