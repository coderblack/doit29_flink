package eagle.etl;

import com.alibaba.fastjson.JSON;
import eagle.functions.TrafficAnalyseFunc;
import eagle.pojo.EventBean;
import eagle.pojo.TrafficBean;
import eagle.utils.SqlHolder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/7
 **/
public class AppUserRetentionAnalyse {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 构造一个 kafka 的source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("dwd-applog-detail")
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("tr")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // 从kafka的dwd明细topic读取数据
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "dwd-applog");

        // 带上事件时间语义和watermark生成策略的  bean对象数据流
        SingleOutputStreamOperator<EventBean> beanStream = sourceStream.map(json -> JSON.parseObject(json, EventBean.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));

        SingleOutputStreamOperator<TrafficBean> trafficStream = beanStream.keyBy(bean -> bean.getGuid())
                .process(new TrafficAnalyseFunc());


        //trafficStream.print();

        // 在后面继续写统计逻辑
        // 把上面datatream注册成flinsql的表
        tenv.createTemporaryView("traffic", trafficStream, Schema.newBuilder()
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")  // 声明一个逻辑字段（来自数据流中的一个元数据rowtime）
                .watermark("rt", "rt")  // watermark的生成策略： 基于rt字段，且不允许乱序
                .build());

        /**
         *- * - 用户活跃度实时分析 dwd
         *    * 分析需求举例：
         *    * 各渠道、手机型号  的 新用户到1日留存数
         *    * 各渠道、手机型号  的 新用户到2日留存数
         *    * 各渠道、手机型号  的 新用户到3日留存数
         *    * 各渠道、手机型号  的 新用户到7日留存数
         */
        tenv.executeSql(SqlHolder.NEW_RETENTION_ANA).print();

        env.execute();
    }
}
