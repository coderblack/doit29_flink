package eagle.functions;

import eagle.pojo.AreaBean;
import eagle.pojo.EventBean;
import eagle.utils.HbaseConnUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DimensionKeyedProcessFunction extends KeyedProcessFunction<String, EventBean,EventBean> {
    Connection hbaseConn;
    Table geoTable;
    MapState<String, AreaBean> geoState;
    OutputTag<String> unknownGps;


    @Override
    public void open(Configuration configuration) throws Exception {
        // 创建hbase维表连接
        hbaseConn = HbaseConnUtil.getConn();
        geoTable = hbaseConn.getTable(TableName.valueOf("eagle_geo_dict"));

        // 创建状态存储器  {geo->省市区信息}
        geoState = getRuntimeContext().getMapState(new MapStateDescriptor<String, AreaBean>("geoState", String.class, AreaBean.class));

        // 创建一个查询不到地理位置的gps坐标的测流 标记
        unknownGps = new OutputTag<>("unknown_gps", TypeInformation.of(String.class));
    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {
        String province ="未知";
        String city ="未知";
        String region ="未知";
        if(StringUtils.isNotBlank(eventBean.getGeoHashCode())) {
            // 先从状态中查询地理位置信息
            AreaBean areaBean = geoState.get(eventBean.getGeoHashCode());
            if(areaBean != null ){
                // 填充字段
                province = areaBean.getProvince();
                city = areaBean.getCity();
                region = areaBean.getRegion();
            }else {
                // 如果状态中没有，则去hbase中查，并将查询到的结果缓存到状态中
                Get get = new Get(Bytes.toBytes(eventBean.getGeoHashCode()));
                Result result = geoTable.get(get);

                if(!result.isEmpty()) {
                    // 判断hbase的查询结果是否存在，如果存在，填充字段，并缓存到state中
                    province = Bytes.toString(result.getValue("f".getBytes(), "p".getBytes()));
                    city = Bytes.toString(result.getValue("f".getBytes(), "c".getBytes()));
                    region = Bytes.toString(result.getValue("f".getBytes(), "r".getBytes()));

                    // 缓存到state中
                    geoState.put(eventBean.getGeoHashCode(),new AreaBean(eventBean.getGeoHashCode(),province,city,region));

                }else {
                    // 如果不存在对应的参考点，则将当前的gps坐标放入一个测流输出，在另外一条线去请求高德来丰富地理位置维表数据
                    context.output(unknownGps,eventBean.getLatitude()+","+eventBean.getLongitude());
                }

            }

        }

        // 填充结果字段
        eventBean.setProvince(province);
        eventBean.setCity(city);
        eventBean.setRegion(region);

        // 输出
        collector.collect(eventBean);

    }

    @Override
    public void close() throws Exception {
        geoTable.close();
        hbaseConn.close();
    }
}
