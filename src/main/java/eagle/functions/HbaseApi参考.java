package eagle.functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HbaseApi参考 {

    // hbase  根据 设备id，去 “设备-账号”绑定表中，查询 绑定权重最高的账号
    // hbase中 “设备-账号”  ：  rowKey：设备id   family: f    qualifier: q    value:"account01:1000,account2:800"
    // hbase(main):001:0> create 'eagle_device_account_bind','f'
    // hbase(main):001:0> put 'eagle_device_account_bind','d001','f:q','abc:100,bbb:80'

    public static void getAccount() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doit01:2181,doit02:2181,doit03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        // 查询  一个指定 设备id的 对应  账号
        Table table = conn.getTable(TableName.valueOf("eagle_device_account_bind"));

        Get get = new Get("d001".getBytes());
        Result result = table.get(get);

        if(!result.isEmpty()){
            result.advance();
            // value:{account01:1000,account2:800}
            byte[] valueBytes = result.getValue("f".getBytes(), "q".getBytes());
            String value = new String(valueBytes);
            // 取其中分数最高的账号
            System.out.println(value);  // 我这里只是给你api
        }

    }


    /**
     * 计数器递增api
     * @throws IOException
     */
    public static void incrCounter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doit01:2181,doit02:2181,doit03:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        // hbase(main):001:0> create 'eagle_counter','f'

        Table table = conn.getTable(TableName.valueOf("eagle_counter"));
        long c = table.incrementColumnValue("r".getBytes(), "f".getBytes(), "c".getBytes(), 1);
        System.out.println(c);
    }

    public static void main(String[] args) throws IOException {

        incrCounter();

    }


}
