package eagle.etl;

import com.alibaba.fastjson.JSON;
import eagle.functions.TrafficAnalyseFunc;
import eagle.pojo.EventBean;
import eagle.pojo.TrafficBean;
import eagle.utils.SqlHolder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/6
 **/
public class ApplogTrafficAnalyseV2 {
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
        tenv.createTemporaryView("traffic",trafficStream, Schema.newBuilder()
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3),"rowtime")  // 声明一个逻辑字段（来自数据流中的一个元数据rowtime）
                .watermark("rt","rt")  // watermark的生成策略： 基于rt字段，且不允许乱序
                .build());

        //tenv.executeSql("desc traffic").print();

        //tenv.executeSql("select * from traffic").print();

        /**
         * -- 流量分析主题：分析需求举例：  ->
         *    今日截止到当前的                 uv,pv,访问时长,访问次数
         *    今日截止到当前，各渠道的          uv,pv,访问时长,访问次数
         *    今日截止到当前，各渠道的新用户的   uv, pv,访问时长,访问次数
         *    今日截止到当前，各类终端型号的     uv,pv,访问时长,访问次数
         *    今日截止到当前，各省市区的        uv,pv,访问时长,访问次数
         *
         *    guid,会话id,切割会话id,行为时间,事件id,所在页面id,所在页面的加载时间,省,市,区,设备类型,新老属性
         *     1  , s1  , s1:t1   , t1    ,applaunch   , null  ,  t1       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t1   , t2    ,pageload   , pg001  ,  t2       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t1   , t3    , e1        , pg001  ,  t2       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t1   , t4    ,putback    , pg001  ,  t2       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t5    ,wakeup     , pg001  ,  t5       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t6    ,flag       , pg001  ,  t5       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t6    ,pageload   , pg003  ,  t6       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t7    , e1        , pg003  ,  t6       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t8    , e1        , pg003  ,  t6       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t9    ,flag       , pg003  ,  t6       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t9    ,pageload   , pg006  ,  t9       , 江苏省,苏州市,园林区, iphone8, 1
         *     1  , s1  , s1:t5   , t10   ,appclose   , pg006  ,  t9       , 江苏省,苏州市,园林区, iphone8, 1
         *  =>  先计算中间结果：  每个人，每次切割会话，会话时长，访问的pv数
         *  =>  在计算最终结果：  count(distinct guid) ,sum(pv) , sum(会话时长）,count(1)
         */
        // 先做中间聚合： 按用户和会话，来统计会话中的pv数，访问时长
        tenv.executeSql(SqlHolder.TRAFFIC_AGG_USER_SESSION);
        //tenv.executeSql("select * from  traffic_agg_user_session").print();

        // 可以将上面的用户、会话轻度聚合结果表数据，插入doris
        //tenv.executeSql("insert into doris_traffic_agg_user_session select  * from traffic_agg_user_session");

        // 针对上面的轻度聚合结果表，继续聚合最终大屏展示要用的报表
        //tenv.executeSql(SqlHolder.TRAFFIC_DIM_ANA_01);

        // 可以将上面统计的多维报表，落地到doris提供大屏展示的查询
        //tenv.executeSql("insert into doris_traffic_dim_ana_01 select  * from traffic_dim_ana_01");


        /**
         * 页面分析，报表开发
         *   今日截止到当前，今日热推商品的pv、uv、访问次数、时长
         *   今日截止到当前，每个页面上的pv数，会话数，停留时长
         *   今日截止到当前，pv数最高的前20个页面
         *   今日截止到当前，uv数最高的前20个页面
         *   今日截止到当前，停留时长最长的前20个页面
         */
        // 先统计每个页面的停留时长，会话数，pv数 ,将结果创建成一个临时视图 pageStatistic
        tenv.executeSql(SqlHolder.Page_STAT_AGG);
        // 统计截止到当前，pv数最大的前20个页面
        tenv.executeSql("select * from pageStatistic order by pagePv desc  limit 20");
        // 统计截止到当前，uv数最大的前20个页面
        tenv.executeSql("select * from pageStatistic order by pageUv desc  limit 20");
        // 统计截止到当前，停留时长最大的前20个页面
        tenv.executeSql("select * from pageStatistic order by pageTimeLong desc  limit 20");


        /**
         * 流量分析，累计窗口统计
         *   今日每一个小时段，各渠道的             uv ,pv,累计访问时长和访问次数
         *   今日每一个小时段，各渠道的新、老        uv ,pv,累计访问时长和访问次数
         *   今日每一个小时段，各类终端型号的        uv ,pv,累计访问时长和访问次数
         *   今日每一个小时段，各类终端型号的        uv ,pv,累计访问时长和访问次数
         *   今日每一个小时段，各省市区的           uv ,pv,累计访问时长和访问次数
         */
        tenv.executeSql(SqlHolder.TRAFFIC_DIM_ANA_02);













        env.execute();
    }
}
