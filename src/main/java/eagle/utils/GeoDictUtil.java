package eagle.utils;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.*;

/**
 * hbase中的geohash码字典表创建
 * hbase> create 'eagle_geo_dict' ,'f'
 */
public class GeoDictUtil {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doit01:2181,doit02:2181,doit03:2181");
        org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(conf);
        Table table = hbaseConn.getTable(TableName.valueOf("eagle_geo_dict"));


        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/realtimedw?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai", "root", "123456");

        Statement statement = conn.createStatement();
        String sql = "SELECT\n" +
                "   lev_1.AREANAME  as province,\n" +
                "   lev_2.AREANAME  as city,\n" +
                "   lev_3.AREANAME  as region,\n" +
                "   lev_4.BD09_LAT  as lat,\n" +
                "   lev_4.BD09_LNG  as lng \n" +
                "from t_md_areas lev_4  join t_md_areas lev_3\n" +
                "on lev_4.`LEVEL`=4 and lev_4.PARENTID = lev_3.ID\n" +
                "join t_md_areas lev_2 on lev_3.PARENTID = lev_2.ID\n" +
                "join t_md_areas lev_1 on lev_2.PARENTID = lev_1.ID\n" +
                "\n" +
                "UNION ALL\n" +
                "\n" +
                "SELECT\n" +
                "   lev_1.AREANAME  as province,\n" +
                "   lev_2.AREANAME  as city,\n" +
                "   lev_3.AREANAME  as region,\n" +
                "   lev_3.BD09_LAT  as lat,\n" +
                "   lev_3.BD09_LNG  as lng \n" +
                "from t_md_areas lev_3  \n" +
                "join t_md_areas lev_2 on lev_3.`LEVEL`=3 and lev_3.PARENTID = lev_2.ID\n" +
                "join t_md_areas lev_1 on lev_2.PARENTID = lev_1.ID\n";

        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()){
            String province = resultSet.getString("province");
            String city = resultSet.getString("city");
            String region = resultSet.getString("region");
            double lat = resultSet.getDouble("lat");
            double lng = resultSet.getDouble("lng");


            // 把参考点的经纬度坐标，转成geohash码
            String geoCode = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5);


            // 往hbase插入
            Put put = new Put(Bytes.toBytes(geoCode));
            put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("p"),Bytes.toBytes(province));
            put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("c"),Bytes.toBytes(city));
            put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("r"),Bytes.toBytes(region));


            table.put(put);
        }


        resultSet.close();
        conn.close();

        table.close();
        hbaseConn.close();


    }

}
