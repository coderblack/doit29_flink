package cn.doitedu.flink.sqls;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/5
 **/
public class TrafficAnalyse {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("doit01", 9999);
        SingleOutputStreamOperator<Traffic.Bean> stream = s1.filter(StringUtils::isNotBlank).map(s -> JSON.parseObject(s, Traffic.Bean.class)).returns(Traffic.Bean.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Traffic.Bean>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<Traffic.Bean>) (element, recordTimestamp) -> element.getTs()))
                .keyBy(Traffic.Bean::getGuid)
                .process(new AnalyseProcessFunc());


        stream.print();

        env.execute();
    }
}
