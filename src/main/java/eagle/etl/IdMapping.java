package eagle.etl;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import eagle.functions.DimensionKeyedProcessFunction;
import eagle.functions.IdMappingFunction;
import eagle.pojo.EventBean;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.awt.*;
import java.util.Date;

public class IdMapping {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取kafka中的用户行为日志数据流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("eagle-001")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("eagle-applog")
                .build();

        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");

        // 真实数据：{"account":"gesw,azt","appid":"cn.doitedu.study.Yiee","appversion":"8.3","carrier":"360移动","deviceid":"CGDNHCPWUTQQ","devicetype":"IPHONE-8","eventid":"launch","ip":"155.74.103.111","latitude":34.756984446036,"longitude":113.65004531926762,"nettype":"WIFI","osname":"ios","osversion":"8.8","properties":{},"releasechannel":"酷市场-CoolMar","resolution":"1024*768","sessionid":"vtlmbxdo","timestamp":1645968611380}
        SingleOutputStreamOperator<EventBean> stream2 = sourceStream.map(new MapFunction<String, EventBean>() {
            @Override
            public EventBean map(String json) throws Exception {

                EventBean eventBean = JSON.parseObject(json, EventBean.class);

                return eventBean;
            }
        });


        // 清洗过滤
        SingleOutputStreamOperator<EventBean> resultStream = stream2.filter(bean -> StringUtils.isNotBlank(bean.getDeviceid())
                        && StringUtils.isNotBlank(bean.getEventid())
                        && bean.getTimestamp() > 1000000000000L
                        && bean.getProperties() != null
                )
                // 按设备号分区
                .keyBy(bean -> bean.getDeviceid())
                // idmapping映射
                .process(new IdMappingFunction())
                // 新老属性标记
                .map(bean -> {
                    long firstAccessTime = bean.getFirstAccessTime();
                    long registerTime = bean.getRegisterTime();
                    // 判断上面的两个时间任意一个不是今天，则是老用户
                    long judeTime = firstAccessTime != 0 ? firstAccessTime : registerTime;

                    Date judeDate = new Date(judeTime);
                    Date today = new Date();

                    boolean sameDay = DateUtils.isSameDay(today, judeDate);

                    bean.setIsNew(sameDay ? 1 : 0);

                    return bean;
                }).returns(EventBean.class)
                // 关联hbase中的geohash地理位置维表、终端设备信息维表
                // 考虑到算子状态的不便利（只有ListState），此处打算用keyedState
                // 但是要用keyedState，就必须是在KeyedStream上下文中（必须keyBy之后）
                // 需要设计一个合适的key，综合考虑后，用geohash码的前2位
                .map(bean -> {
                    double lat = bean.getLatitude();
                    double lng = bean.getLongitude();
                    String geo = "";
                    try {
                        geo = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // 根据gps坐标生成 geohash码，并放入数据封装bean
                    bean.setGeoHashCode(geo);
                    return bean;
                }).returns(EventBean.class)
                // 根据geohash码的前2位进行keyBy
                .keyBy(bean -> bean.getGeoHashCode().substring(0, 2))
                // 进行地理位置维度信息（设备型号终端属性信息）等关联
                .process(new DimensionKeyedProcessFunction());   //  在本算子中，会将那些查不到地理位置的gps坐标，输出到测流

        // 获取测流
        DataStream<String> unknownGpsStream = resultStream.getSideOutput(new OutputTag<String>("unknown_gps", TypeInformation.of(String.class)));

        // 构造一个用于接收“未知gps坐标”的kafka sink
        KafkaSink<String> unknownGpsSink = KafkaSink.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic("unknown-gps")
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 将测流数据，写入kafka sink
        unknownGpsStream.sinkTo(unknownGpsSink);
        //unknownGpsStream.print("unknown_gps");


        resultStream.print("dwd_stream");


        env.execute();

    }
}