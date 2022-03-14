package cn.doitedu.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/11
 **/
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s1 = env.socketTextStream("doit01", 20999);


        StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path("hdfs://doit01:8020/flink-demo/"), new SimpleStringEncoder<String>()).build();

        s1.map(s->{
            JSONObject obj = JSON.parseObject(s);
            String w = obj.getString("w");
            Integer c = obj.getInteger("c");
            return Tuple2.of(w,c);
        }).returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(t->t.f0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return value.f0+","+value.f1;
                    }
                }).addSink(sink);
        env.execute();
    }
}
