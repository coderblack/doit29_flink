package eagle.demos;

import io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/3
 * doris 连接器演示demo：
 * 需求： 从kafka中读取topic_table1的数据后，写入doris db2.table1
 **/
public class DoirsConnector {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建source表 映射 kafka中的topic
        tenv.executeSql("CREATE TABLE kfk_table1 (\n" +
                "  `siteid` INT,\n" +
                "  `citycode` SMALLINT,\n" +
                "  `username` STRING,\n" +
                "  `pv` BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_table1',\n" +
                "  'properties.bootstrap.servers' = 'doit01:9092,doit02:9092,doit03:9092',\n" +
                "  'properties.group.id' = 'kfk01',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");

        // 建sink表 ，映射 doris中的一张目标表
        tenv.executeSql("CREATE TABLE doris_table1 (\n" +
                "  `siteid` INT,\n" +
                "  `citycode` SMALLINT,\n" +
                "  `username` STRING,\n" +
                "  `pv` BIGINT\n" +
                ") WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'doit01:8030',\n" +
                "      'table.identifier' = 'db2.table1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123456'\n" +
                ")");


        // 从source表查询数据后，写入sink表
        tenv.executeSql("insert into doris_table1 select siteid,citycode,username,pv from kfk_table1");

    }
}
