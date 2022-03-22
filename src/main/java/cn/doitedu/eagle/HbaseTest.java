package cn.doitedu.eagle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/21
 **/
public class HbaseTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doit01:2181,doit02:2181,doit03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("device_guid"));

        /*Put put1 = new Put("a".getBytes());
        put1.addColumn("f".getBytes(StandardCharsets.UTF_8),"guid".getBytes(StandardCharsets.UTF_8), Bytes.toBytes(10L));

        Put put2 = new Put("b".getBytes());
        put2.addColumn("f".getBytes(StandardCharsets.UTF_8),"guid".getBytes(StandardCharsets.UTF_8), Bytes.toBytes(2L));

        Put put3 = new Put("c".getBytes());
        put3.addColumn("f".getBytes(StandardCharsets.UTF_8),"guid".getBytes(StandardCharsets.UTF_8), Bytes.toBytes(3L));

        Put put4 = new Put("d".getBytes());
        put4.addColumn("f".getBytes(StandardCharsets.UTF_8),"guid".getBytes(StandardCharsets.UTF_8), Bytes.toBytes(4L));


        List<Put> puts = Arrays.asList(put1);

        table.put(puts);*/


        /*Result result = table.get(new Get(Bytes.toBytes("a")));
        if(!result.isEmpty()) {
            result.advance();
            Cell current = result.current();
            byte[] valueArray = current.getValueArray();
            System.out.println(Bytes.toInt(valueArray));
        }

        table.close();
        conn.close();*/

        ZooKeeper zk = new ZooKeeper("doit01:2181,doit02:2181,doit03:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });

        try {
           /* String path = zk.create("/lock/a", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);*/
            zk.delete("/lock/a",-1);
            //System.out.println(path);
        }catch (Exception e){
            System.out.println(e.getClass());
        }


    }
}
