package cn.doitedu.flink.doris;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisTest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        String mysql_cdc = "CREATE TABLE mysql_table1 (\n" +
                "     user_id BIGINT,\n" +
                "     gender STRING,\n" +
                "     city STRING,\n" +
                "     age SMALLINT ,\n" +
                "     PRIMARY KEY(user_id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'abc',\n" +
                "     'table-name' = 'mysql_table1')";
        tenv.executeSql(mysql_cdc);
        tenv.executeSql("select * from mysql_table1").print();

        String read = "CREATE TABLE table1 (\n" +
                "    siteid INT,\n" +
                "    citycode SMALLINT,\n" +
                "    username STRING,\n" +
                "    pv BIGINT\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = 'doit01:8030',\n" +
                "      'table.identifier' = 'db1.table1',\n" +
                "      'username' = 'root', \n" +
                "      'password' = '' \n" +
                ")";
        // tenv.executeSql(read);
        // tenv.executeSql("insert into table1 select * from mysql_table1")



    }
}
