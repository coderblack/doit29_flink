package eagle.functions;

import eagle.pojo.AccountIdMapBean;
import eagle.pojo.DeviceIdMapBean;
import eagle.pojo.EventBean;
import eagle.utils.HbaseConnUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * -- 测试用 mysql 用户注册信息表：
 * CREATE TABLE `ums_member` (
 * `id` int(11) NOT NULL AUTO_INCREMENT,
 * `account` varchar(255) DEFAULT NULL,
 * `create_time` bigint(20) DEFAULT NULL,
 * `update_time` bigint(20) DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;



 * -- 设备账号绑定权重关系表，hbase创建
 * hbase>  create 'device_account_bind','f'
 *
 * -- 表结构说明
 * rowKey:deviceId
 * family: "f"
 * qualifier : 账号
 * value:  评分
 * ----------------------------------
 * rk        |      f              |
 * ---------------------------------
 * dev01     | ac01:100, ac02:80 |
 * ----------------------------------

 * -- 设备临时GUID表，hbase创建
 * hbase>  create 'device_tmp_guid','f'
 * -- 表结构说明
 * rowKey:deviceId
 * family: "f"
 * qualifier : "guid"
 * value:  100000001
 * qualifier: ft  -- 首次到访时间
 * value:  137498587283123
 * ----------------------------------------------------
 * rk      |      f                        |
 * ------------------------------------------------------
 * dev01     | guid:1001,ft:137498587283123 |
 * -------------------------------------------------------


 * -- 设备临时GUID计数器表，hbase创建
 * hbase>  create 'device_tmp_maxid','f'
 *
 * -- 表结构说明
 * rowKey: "r"
 * family: "f"
 * qualifier : "maxid"
 * value:  100000001
 *
 * ----------------------------------------------------
 * rk      |      f                        |
 * ------------------------------------------------------
 * r      | maxId:1000001                 |
 * -------------------------------------------------------
 *
 * -- 放入1亿这个初始值的客户端命令
 * incr 'device_tmp_maxid','r','f:maxid',100000000
 *
 */

public class IdMappingFunction extends KeyedProcessFunction<String, EventBean, EventBean> {

    MapState<String, AccountIdMapBean> accountIdMapState;
    MapState<String, DeviceIdMapBean> deviceIdMapState;
    Connection conn;
    PreparedStatement preparedStatement;
    org.apache.hadoop.hbase.client.Connection hbaseConn;
    Table deviceBindTable;
    Table deviceTmpGuidTable;
    Table deviceTmpMaxIdTable;


    @Override
    public void open(Configuration parameters) throws Exception {

        // 构造一个用于存储  账号->guid 信息的本地状态
        MapStateDescriptor<String, AccountIdMapBean> accountIdMapStateDescriptor = new MapStateDescriptor<>("account_idmp_state", String.class, AccountIdMapBean.class);
        accountIdMapState = getRuntimeContext().getMapState(accountIdMapStateDescriptor);

        // 构造一个用于存储  设备号->guid 信息的本地状态
        MapStateDescriptor<String, DeviceIdMapBean> deviceIdMapStateDescriptor = new MapStateDescriptor<>("device_idmp_state", String.class, DeviceIdMapBean.class);
        deviceIdMapState = getRuntimeContext().getMapState(deviceIdMapStateDescriptor);


        // 构造一个jdbc的连接（并不需要用连接池）
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/eagle?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai", "root", "123456");
        preparedStatement = conn.prepareStatement("select * from ums_member where account = ?");


        // 构造一个hbase的连接
        hbaseConn = HbaseConnUtil.getConn();

        // 获取  设备账号绑定表
        deviceBindTable = this.hbaseConn.getTable(TableName.valueOf("device_account_bind"));

        // 获取  设备临时guid表
        deviceTmpGuidTable = this.hbaseConn.getTable(TableName.valueOf("device_tmp_guid"));

        // 获取  设备临时guid最大值计数器表
        deviceTmpMaxIdTable = this.hbaseConn.getTable(TableName.valueOf("device_tmp_maxid"));

    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {

        String userAccount = eventBean.getAccount();


        // 判断是否有账号，如果有，则用账号在state中查询信息，如果state中没有，则去mysql中查询注册信息  ，并且查完后，放入state
        if (StringUtils.isNotBlank(userAccount)) {
            AccountIdMapBean accountIdMapBean = accountIdMapState.get(userAccount);
            // 如果在状态中查询到了用户的guid、注册时间等信息，则填充结果字段并输出返回
            long guid;
            long registerTime;

            if (accountIdMapBean != null) {
                guid = accountIdMapBean.getGuid();
                registerTime = accountIdMapBean.getRegisterTime();
            }
            // 如果在状态中没有查询到，则去mysql的用户注册表中查询信息
            else {
                preparedStatement.setString(1, userAccount);
                ResultSet resultSet = preparedStatement.executeQuery();
                resultSet.next();
                guid = resultSet.getLong("id");
                registerTime = resultSet.getLong("create_time");

                // 将查询到的guid和注册时间，填入 state
                accountIdMapState.put(userAccount, new AccountIdMapBean(guid, registerTime));
            }


            // 填充查询到的guid和注册时间
            eventBean.setGuid(guid);
            eventBean.setRegisterTime(registerTime);


            // 为该设备号的  设备账号绑定关系  增加 本次账号的 权重
            deviceBindTable.incrementColumnValue(Bytes.toBytes(eventBean.getDeviceid()), Bytes.toBytes("f"), Bytes.toBytes(userAccount), 1);
            // 并对其他绑定账号的权重进行衰减
            Get get = new Get(Bytes.toBytes(eventBean.getDeviceid()));
            Result result = deviceBindTable.get(get);
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                // 取到绑定账号
                String bindAccount = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (!bindAccount.equals(userAccount)) {
                    deviceBindTable.incrementColumnValue(Bytes.toBytes(eventBean.getDeviceid()), Bytes.toBytes("f"), Bytes.toBytes(bindAccount), -1);
                }
            }


            // 输出
            collector.collect(eventBean);
        }

        // 如果没有账号
        else {
            // 则用deviceid去state中查询，如果state中没有，则去hbase查询设备所绑定的账号，然后再查mysql，并且查完后，放入state
            DeviceIdMapBean deviceIdMapBean = deviceIdMapState.get(eventBean.getDeviceid());

            // 如果状态中有该设备号的信息
            if (deviceIdMapBean != null) {
                // 填充字段
                eventBean.setGuid(deviceIdMapBean.getGuid());
                eventBean.setFirstAccessTime(deviceIdMapBean.getFirstAccessTime());

                // 输出结果
                collector.collect(eventBean);
            }
            // 如果状态中没有该设备号的信息
            else {
                byte[] deviceIdBytes = Bytes.toBytes(eventBean.getDeviceid());
                // 根据设备号，去hbase的 “设备-账号 绑定表” 查询账号
                Get get = new Get(deviceIdBytes);
                Result result = deviceBindTable.get(get);

                // 如果在hbase中存在设备绑定的账号信息
                if (!result.isEmpty()) {
                    CellScanner cellScanner = result.cellScanner();

                    String tmpAccount = "";
                    long tmpWeight = 0;
                    // 迭代这个设备所绑定的所有账号及其权重，取权重最大的
                    while (cellScanner.advance()) {
                        Cell cell = cellScanner.current();
                        // 取账号
                        byte[] accountBytes = CellUtil.cloneQualifier(cell);
                        // 取权重
                        byte[] weightBytes = CellUtil.cloneValue(cell);
                        long weight = Bytes.toLong(weightBytes);

                        if (weight > tmpWeight) {
                            tmpWeight = weight;
                            tmpAccount = Bytes.toString(accountBytes);
                        }

                    }

                    // 拿着个权重最大的账号，在状态中查询，如果状态中没有，则去mysql中查
                    AccountIdMapBean accountIdMapBean = accountIdMapState.get(tmpAccount);
                    // 如果在状态中查询到了用户的guid、注册时间等信息，则填充结果字段并输出返回
                    long guid;
                    long registerTime;

                    if (accountIdMapBean != null) {
                        guid = accountIdMapBean.getGuid();
                        registerTime = accountIdMapBean.getRegisterTime();
                    }

                    // 如果在状态中没有查询到，则去mysql的用户注册表中查询信息
                    else {
                        preparedStatement.setString(1, tmpAccount);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        guid = resultSet.getLong("id");
                        registerTime = resultSet.getLong("create_time");

                        // 将查询到的guid和注册时间，填入 state
                        accountIdMapState.put(tmpAccount, new AccountIdMapBean(guid, registerTime));
                    }


                    // 填充查询到的guid和注册时间
                    eventBean.setGuid(guid);
                    eventBean.setRegisterTime(registerTime);
                    eventBean.setAccount(tmpAccount);  // 顺便为日志数据补上账号


                    // 输出结果
                    collector.collect(eventBean);
                }
                // 如果 hbase 中没有设备绑定的账号信息，则去  deviceid临时guid表 查询
                else {
                    Get get1 = new Get(deviceIdBytes);
                    Result result1 = deviceTmpGuidTable.get(get1);

                    // 如果存在临时id信息
                    if (!result1.isEmpty()) {
                        byte[] tmpGuidBytes = result1.getValue(Bytes.toBytes("f"), Bytes.toBytes("guid"));
                        byte[] firstAccessTimeBytes = result1.getValue(Bytes.toBytes("f"), Bytes.toBytes("ft"));

                        // 填充字段
                        long guid = Bytes.toLong(tmpGuidBytes);
                        long firstAccessTime = Bytes.toLong(firstAccessTimeBytes);
                        eventBean.setGuid(guid);
                        eventBean.setFirstAccessTime(firstAccessTime);


                        // 将查询到的结果，存到state中
                        deviceIdMapState.put(eventBean.getDeviceid(), new DeviceIdMapBean(guid, firstAccessTime));

                        // 输出
                        collector.collect(eventBean);
                    }
                    // 如果不存在临时id信息，则请求 hbase的技术进行增1，得到guid，并将结果插入 deviceid临时guid表
                    else {
                        long newMaxId = deviceTmpMaxIdTable.incrementColumnValue("r".getBytes(), "f".getBytes(), "maxid".getBytes(), 1);
                        long firstAccessTime = eventBean.getTimestamp();

                        // 填充字段
                        eventBean.setGuid(newMaxId);
                        eventBean.setFirstAccessTime(firstAccessTime);

                        // 插入hbase的设备临时guid表
                        Put put = new Put(Bytes.toBytes(eventBean.getDeviceid()));
                        put.addColumn("f".getBytes(), "guid".getBytes(), Bytes.toBytes(newMaxId));
                        put.addColumn("f".getBytes(), "ft".getBytes(), Bytes.toBytes(firstAccessTime));
                        deviceTmpGuidTable.put(put);

                        // 将该设备对应的临时guid，放到flink的本地state中
                        deviceIdMapState.put(eventBean.getDeviceid(), new DeviceIdMapBean(newMaxId, firstAccessTime));

                        // 输出
                        collector.collect(eventBean);
                    }


                }
            }

        }



    }


    @Override
    public void close() throws Exception {
        preparedStatement.close();
        conn.close();

        deviceBindTable.close();
        deviceTmpGuidTable.close();
        deviceTmpMaxIdTable.close();
        hbaseConn.close();

    }
}