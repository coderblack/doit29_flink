package cn.doitedu.flink.sqls;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class SqlDemo5 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceTable =
                "CREATE TABLE sourceTable (\n" +
                "  `user_id` INT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'  ," +
                "   `proctime` as PROCTIME()  \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'behavior',\n" +
                "  'properties.bootstrap.servers' = 'doit01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv' ,\n" +
                "  'csv.ignore-parse-errors' = 'true'" +
                ")";


        String mySqlTable = "CREATE TABLE user_table (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED  \n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/abc',\n" +
                "   'table-name' = 'user_table' ,\n" +
                "   'username' = 'root' , \n" +
                "   'password' = '123456' \n" +
                ") ";

        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(mySqlTable);
        /*tableEnv.executeSql("create table user as select cast(id as bigint) as id,name from user_table");*/


        String sql1 = "insert into sinkTable\n" +
                "select\n" +
                "    item_id\n" +
                "   ,behavior\n" +
                "   ,count(1) as cnt\n" +
                "from sourceTable\n" +
                "group by item_id,behavior";

        String sql2 = " SELECT * FROM sourceTable  \n" +
                "   LEFT JOIN user_table FOR SYSTEM_TIME AS OF sourceTable.`proctime`  \n" +
                "   ON sourceTable.user_id = user_table.id  ";

        String sqla = "select * from sourceTable";
        String sqlb = "select * from user_table";
        Table table = tableEnv.sqlQuery(sql2);

        tableEnv.createTemporaryView("res",table);

        tableEnv.sqlQuery("select user_id,item_id,behavior,name from res").execute().print();


    }


}
