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
public class MySqlCdc2Doris {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt/");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
        tenv.executeSql(" CREATE TABLE flink_stu (\n" +
                "        id INT,\n" +
                "        name string,\n" +
                "        gender STRING,\n" +
                "        score double,\n" +
                "        PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'doit01',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'ABC123.abc123',\n" +
                "     'database-name' = 'abc',\n" +
                "     'table-name' = 'flink_stu')");

        // TODO 做一个维表关联的测试
        // TODO 再去读取另外一份 kafka的数据，里面有 id,phone，然后去关联上面的mysql-cdc得到的数据，用temporal（现世代）关联


        // 查询 插入
        // tenv.executeSql("select * from flink_stu").print();


        // 创建doris的连接器表
        tenv.executeSql("CREATE TABLE doris_stu (\n" +
                "  `id` INT,\n" +
                "  `name` varchar(20),\n" +
                "  `gender` varchar(20),\n" +
                "  `score` double \n" +
                ") WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'doit01:8030',\n" +
                "      'table.identifier' = 'db2.stu',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123456'\n" +
                ")");

        // 将mysql-cdc所得到的数据，查询后插入到doris的表中
        tenv.executeSql("insert into doris_stu select id,name,gender,score from flink_stu");


    }
}
