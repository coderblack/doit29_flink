package cn.doitedu.eagle;

import lombok.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class _02_GuidMapping {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<UserEvent> s1 = source.map(new MapFunction<String, UserEvent>() {
            @Override
            public UserEvent map(String value) throws Exception {
                String[] split = value.split(",");

                return new UserEvent(split[0], split[1], null);
            }
        });

        s1.process(new ProcessFunction<UserEvent, UserEvent>() {
            Connection conn ;
            Table device_guid ;
            Table account_guid ;
            Table device_tmpid ;
            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum","doit01:2181,doit02:2181,doit03:2181");
                conn = ConnectionFactory.createConnection(conf);
                account_guid = conn.getTable(TableName.valueOf("account_guid"));
                device_guid = conn.getTable(TableName.valueOf("device_guid"));
                device_tmpid = conn.getTable(TableName.valueOf("device_tmpid"));
            }

            @Override
            public void processElement(UserEvent value, Context ctx, Collector<UserEvent> out) throws Exception {


            }

            @Override
            public void close() throws Exception {
                device_guid.close();
                device_tmpid.close();
                conn.close();
                super.close();
            }
        });




    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class UserEvent{
        private String deviceId;
        private String account;
        private Long guid;

    }

}
