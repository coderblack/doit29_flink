package eagle.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnUtil {

    public static Connection getConn() throws IOException {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doit01:2181,doit02:2181,doit03:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

}
