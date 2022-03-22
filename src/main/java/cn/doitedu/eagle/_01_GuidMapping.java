package cn.doitedu.eagle;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class _01_GuidMapping {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String hTable = "CREATE TABLE device_guid (\n" +
                " rowkey STRING,\n" +
                " f ROW<guid BIGINT>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'device_guid',\n" +
                " 'zookeeper.quorum' = 'doit01:2181'\n" +
                ")";

        tenv.executeSql(hTable);
        //tenv.executeSql("select * from device_guid").print();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        tenv.createTemporaryView("aclog",source,$("device_id"),$("proctime").proctime());

        tenv.executeSql("select aclog.device_id,aclog.proctime,device_guid.guid \n" +
                "from aclog LEFT JOIN device_guid FOR SYSTEM_TIME AS OF aclog.proctime \n" +
                "ON aclog.device_id = device_guid.rowkey").print();


        env.execute();
    }

}
