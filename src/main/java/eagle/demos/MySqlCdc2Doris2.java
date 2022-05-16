package eagle.demos;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/3
 *
 * 本测试代码，要求
 * 1.  先对mysql开启binlog功能
 *  [root@doit01 ~]# vi /etc/my.cnf
 *   在最后，加上如下配置
       server-id = 1
       log_bin = doitedu
       binlog_format = ROW
       binlog_row_image = FULL
 *  配好之后，重启mysql
 *  [root@doit01 ~]# systemctl restart mysqld
 *
 *  然后用一个mysql的客户端连接上去，用如下sql命令来检查mysql是否成功开启了binlo
 *   mysql> show variables like '%log_bin%';
 *      +---------------------------------+------------------------------+
 *      | Variable_name                   | Value                        |
 *      +---------------------------------+------------------------------+
 *      | log_bin                         | ON                           |
 *      | log_bin_basename                | /var/lib/mysql/doitedu       |
 *      | log_bin_index                   | /var/lib/mysql/doitedu.index |
 *      | log_bin_trust_function_creators | OFF                          |
 *      | log_bin_use_v1_row_events       | OFF                          |
 *      | sql_log_bin                     | ON                           |
 *      +---------------------------------+------------------------------+
 *
 *
 *  2. 在doris库中建好表：  db2.stu
 *
 *      CREATE TABLE IF NOT EXISTS db2.stu
 *          (
 *              `id` INT              ,
 *              `name` VARCHAR(20)    ,
 *              `gender` VARCHAR(20)  ,
 *              `score` DOUBLE
 *          )
 *     UNIQUE KEY(`id`)
 *     DISTRIBUTED BY HASH(id) BUCKETS 2
 *     PROPERTIES("replication_num" = "1");
 *
 *
 **/
public class MySqlCdc2Doris2 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt/");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
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


        //tenv.executeSql("select DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at,3),'yyyy-MM-dd') from mysql_record").print();


        tenv.executeSql("CREATE TABLE doris_record (\n" +
                "`id` bigint,\n" +
                "`user_id` bigint COMMENT '用户ID',\n" +
                "`date` date,\n" +
                "`device_id` String  COMMENT '设备ID',\n" +
                "`idfa` String  COMMENT 'IOS IDFA',\n" +
                "`os` String  COMMENT '设备系统',\n" +
                "`os_version` String  COMMENT '系统版本',\n" +
                "`version` String  COMMENT 'APP版本',\n" +
                "`system` tinyint  COMMENT '1IOS 2安卓',\n" +
                "`platform` tinyint  COMMENT '1APP2小程序3H5',\n" +
                "`event_id` bigint  COMMENT '事件ID',\n" +
                "`log_id` bigint  COMMENT '日志自增ID',\n" +
                "`base_uri` String  COMMENT '当前短路径',\n" +
                "`event_data` String  COMMENT '关联数据额外数据',\n" +
                "`created_at` bigint\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = 'doit01:8030',\n" +
                "  'table.identifier' = 'point.record',\n" +
                "  'sink.batch.size' = '1',\n" +
                "  'sink.batch.interval'='1',\n" +
                "  'username' = 'root',\n" +
                "  'password' = ''\n" +
                ")");



        tenv.executeSql("insert into doris_record\n" +
                "select\n" +
                "`id`,\n" +
                "`user_id`,\n" +
                " TO_DATE(DATE_FORMAT(TO_TIMESTAMP_LTZ(created_at,3),'yyyy-MM-dd')) as `date`,\n" +
                "`device_id`,\n" +
                "`idfa`,\n" +
                "`os`,\n" +
                "`os_version`,\n" +
                "`version`,\n" +
                "`system`,\n" +
                "`platform`,\n" +
                "`event_id`,\n" +
                "`log_id`,\n" +
                "`base_uri`,\n" +
                "`event_data`,\n" +
                "`created_at`\n" +
                "from mysql_record");





    }
}
