package cn.doitedu.flink.sqls;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class _08_JoinQuery {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("localhost", 9998);
        DataStreamSource<String> s2 = env.socketTextStream("localhost", 9999);

        tenv.createTemporaryView("t1",s1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .build());


        tenv.createTemporaryView("t2",s2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .build());

        tenv.executeSql("select * from t1 join t2 on t1.f0 = t2.f0")/*.print()*/;
        tenv.executeSql("select * from t1 left join t2 on t1.f0 = t2.f0")/*.print()*/;
        tenv.executeSql("select * from t1 right join t2 on t1.f0 = t2.f0")/*.print()*/;
        tenv.executeSql("select * from t1 full outer join t2 on t1.f0 = t2.f0").print();

    }
}
