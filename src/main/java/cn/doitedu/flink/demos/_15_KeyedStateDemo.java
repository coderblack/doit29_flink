package cn.doitedu.flink.demos;

import cn.doitedu.flink.functions._15_ChannelEventsCntMapFunc;
import cn.doitedu.flink.functions.Ex1_MapFunc;
import cn.doitedu.flink.functions._15_EventsCntMapFunc;
import cn.doitedu.flink.functions._15_LatestEventsMapFunc;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojos.EventLog;
import scala.Int;

/**
 * 演示各种keyed state的使用
 * 统计各渠道的行为数据条数
 * 1,addcart,app,1646784001000,100
 * 1,addcart,web,1646784002000,100
 * 1,addcart,web,1646784003000,100
 * 1,addcart,wx,1646784004000,100
 * 1,addcart,wx,1646784005000,100
 * 1,addcart,h5,1646784005000,100
 * 1,addcart,h5,1646784005000,100
 */
public class _15_KeyedStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        // 把字符串日志数据，转成EventLog对象数据
        SingleOutputStreamOperator<EventLog> stream1 = source.map(new Ex1_MapFunc());

        // 按渠道keyBy
        KeyedStream<EventLog, String> keyedStream = stream1.keyBy(o -> o.getChannel());

        // 求各渠道的日志条数   // 用了ValueState
        keyedStream.map(new _15_ChannelEventsCntMapFunc()).setParallelism(1).print();


        // 求渠道内各种事件的条数   // 用了MapState
        keyedStream.map(new _15_EventsCntMapFunc()).setParallelism(1).print();

        // 求渠道内最近3条数据的事件列表  // 用了ListState
        keyedStream.map(new _15_LatestEventsMapFunc()).setParallelism(1).print();

        env.execute();

    }
}
