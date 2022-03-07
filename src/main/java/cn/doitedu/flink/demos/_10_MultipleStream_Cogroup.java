package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _10_MultipleStream_Cogroup {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.fromElements("1,aa,m,18","2,bb,m,28","3,cc,f,38");

        DataStreamSource<String> stream2 = env.fromElements("1:aa:m:18","2:bb:m:28","3:cc:f:38");

        /*stream1.coGroup(stream2).where().equalTo().*/




    }

}
