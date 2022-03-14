package cn.doitedu.flink.sqls;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/14
 **/
public class SqlDemo4 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceTable = "CREATE TABLE sourceTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'behavior',\n" +
                "  'properties.bootstrap.servers' = 'doit01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv' ,\n" +
                "  'csv.ignore-parse-errors' = 'true'" +
                ")";

        String sinkTable = "CREATE TABLE sinkTable (\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `cnts` BIGINT ,\n" +
                "  PRIMARY KEY (item_id,behavior) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'item-cnt',\n" +
                "  'properties.bootstrap.servers' = 'doit01:9092',\n" +
                "  'key.format' = 'csv',\n" +
                "  'value.format' = 'csv'\n" +
                ")";

        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(sinkTable);

        tableEnv.executeSql("insert into sinkTable\n" +
                "select\n" +
                "    item_id\n" +
                "   ,behavior\n" +
                "   ,count(1) as cnt\n" +
                "from sourceTable\n" +
                "group by item_id,behavior");



    }


}
