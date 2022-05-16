package cn.doitedu.flink.sqls;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestDoris {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql("CREATE TABLE `mysql_record` (\n" +
                "`id` bigint,\n" +
                "`user_id` bigint COMMENT '用户ID',\n" +
                "`device_id` STRING  COMMENT '设备ID',\n" +
                "`idfa` STRING  COMMENT 'IOS IDFA',\n" +
                "`os` STRING  COMMENT '设备系统',\n" +
                "`os_version` STRING  COMMENT '系统版本',\n" +
                "`version` STRING  COMMENT 'APP版本',\n" +
                "`system` tinyint  COMMENT '1IOS 2安卓',\n" +
                "`platform` tinyint  COMMENT '1APP2小程序3H5',\n" +
                "`event_id` bigint  COMMENT '事件ID',\n" +
                "`log_id` bigint  COMMENT '日志自增ID',\n" +
                "`base_uri` STRING  COMMENT '当前短路径',\n" +
                "`event_data` STRING  COMMENT '关联数据额外数据',\n" +
                "`created_at` bigint ,\n" +
                "primary key(id)  NOT ENFORCED\n" +
                ") WITH ( \n" +
                "  'connector' = 'mysql-cdc', \n" +
                "  'hostname' = 'doit01', \n" +
                "  'port' = '3306', \n" +
                "  'username' = 'root', \n" +
                "  'password' = 'ABC123.abc123', \n" +
                "  'database-name' = 'abc',\n" +
                "  'table-name' = 'record'\n" +
                ")");


        tenv.executeSql("select * from mysql_record").print();





    }
}
