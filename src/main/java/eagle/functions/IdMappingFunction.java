package eagle.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class IdMappingFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple3<String,String,Long>>{
    Connection conn;
    PreparedStatement stmt;
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456");
        stmt = conn.prepareStatement("select id from ums_memeber where account = ?");
    }

    @Override
    public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple3<String, String, Long>>.Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
        // 1. 判断，日志数据中，是否有账号，如果有，则去查mysql中的业务表
        if(StringUtils.isNotBlank(value.f1)){
            stmt.setString(1,value.f1);
            ResultSet resultSet = stmt.executeQuery();
            Long guid = null;
            while(resultSet.next()){
                guid = resultSet.getLong("id");
            }
            out.collect(Tuple3.of(value.f0, value.f1, guid));
        }else{
            // 2. 如果没有账号，则去查hbase中的 设备-账号，绑定表
            // 2.1  如果查到账号了，在去mysql业务表中查询对应的userid作为 结果


            // 2.2 如果没查到账号，则去 hbase中的 空设备-临时id 映射表查 guid


            // 2.2.1 如果查到了，输出结果


            // 2.2.2 如果没查到，则去 对 hbase的计数器 递增，得到递增后的值作为结果输出，并且把结果数据，写入   “空设备-临时id” 表


        }




    }


    @Override
    public void close() throws Exception {
        stmt.close();
        conn.close();
    }
}