package cn.doitedu.eagle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class IncrRunnable implements Runnable {
    List<String> lst;
    String tid;

    public IncrRunnable(List<String> lst, String tid) {
        this.lst = lst;
        this.tid = tid;
    }

    @Override
    public void run() {

        Connection conn = null;
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "doit01:2181,doit02:2181,doit03:2181");
            conn = ConnectionFactory.createConnection(conf);
            Table incrTable = conn.getTable(TableName.valueOf("incr_test1"));
            Table getTable = conn.getTable(TableName.valueOf("incr_get"));

            ZooKeeper zk = new ZooKeeper("doit01:2181", 1000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                }
            });

            for (String deviceId : lst) {
                Result result = getTable.get(new Get(Bytes.toBytes(deviceId)));
                if (!result.isEmpty()) {
                    // 如果存在结果
                    result.advance();
                    byte[] valueArray = result.current().getValueArray();
                    long guid = Bytes.toLong(valueArray);

                    // 则打印返回
                    System.out.printf("成功拿到现成结果,%s ==> %d%n", deviceId, guid);

                } else {
                    // 如果不存在结果
                    // 则准备去获取自增id
                    // 1. 先去zk上获取锁
                    try {
                        // 2.1 如果拿到锁，则去hbase上自增，获得结果
                        System.out.println("准备注册锁： /lock/" + deviceId);
                        String path = zk.create("/lock/" + deviceId, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                        System.out.println(tid + " 成功拿到锁： " + path);
                        long newMaxId = incrTable.incrementColumnValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"), 1);

                        // 2.2获得结果后，插入getTable
                        Put put = new Put(Bytes.toBytes(deviceId));
                        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("guid"), Bytes.toBytes(newMaxId));
                        getTable.put(put);

                        // 2.3返回
                        System.out.printf(tid + "成功获得计数器incr结果： %s ==> %d%n", deviceId, newMaxId);

                    } catch (Exception e) {
                        // 3.1 如果没拿到锁，进入自旋，反复获取结果
                        int i = 0;
                        while (true) {
                            i++;
                            System.out.println(tid + "锁被占用，自旋次数： " + i);
                            Result res = getTable.get(new Get(Bytes.toBytes(deviceId)));

                            // 3.2 一旦获取结果，返回
                            if (!res.isEmpty()) {
                                res.advance();
                                byte[] valueArray = res.current().getValueArray();

                                // 清除掉zk上的锁
                                try {
                                    zk.delete("/lock/" + deviceId, -1);
                                } catch (Exception exception) {

                                }
                                System.out.println(tid + "自旋中拿到结果: " + deviceId + " ===> " + Bytes.toLong(valueArray));
                                break;
                            }
                            Thread.sleep(5);
                        }
                    }
                }
            }


        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
