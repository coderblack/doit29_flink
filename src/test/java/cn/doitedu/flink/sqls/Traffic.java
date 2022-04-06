package cn.doitedu.flink.sqls;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/5
 * 2.1 定时器只在 KeyedStream 上注册
 * 由于定时器是按 key 注册和触发的，因此 KeyedStream 是任何操作和函数使用定时器的先决条件。
 *
 * 2.2 定时器进行重复数据删除
 * TimerService 会自动对定时器进行重复数据的删除，因此每个 key 和时间戳最多只能有一个定时器。这意味着当为同一个 key 或时间戳注册多个定时器时，onTimer() 方法只会调用一次。
 *
 * 2.3 对定时器Checkpoint
 * 定时器也会进行Checkpoint，就像任何其他 Managed State 一样。从 Flink 检查点或保存点恢复作业时，在状态恢复之前就应该触发的定时器会被立即触发。
 *
 * 2.4 删除计时器
 * 从 Flink 1.6 开始，就可以对定时器进行暂停以及删除。如果你使用的是比 Flink 1.5 更早的 Flink 版本，那么由于有许多定时器无法删除或停止，所以可能会遇到检查点性能不佳的问题。
 **/
@Slf4j
public class Traffic {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> s1 = env.socketTextStream("doit01", 9999);
        SingleOutputStreamOperator<Bean> stream = s1.filter(StringUtils::isNotBlank).map(s -> JSON.parseObject(s, Bean.class)).returns(Bean.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bean>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<Bean>) (element, recordTimestamp) -> element.getTs()))
                .keyBy(Bean::getGuid)
                .process(new KeyedProcessFunction<Long, Bean, Bean>() {
                    ValueState<Bean> sidState;
                    ValueState<Long> timerState;
                    ValueState<Tuple2<String,Long>> pageLoadTimeState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sidState = getRuntimeContext().getState(new ValueStateDescriptor<Bean>("sid",Bean.class));
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer",Long.class));
                        pageLoadTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String,Long>>("ploadTime", Types.TUPLE(Types.STRING,Types.LONG)));
                    }

                    @Override
                    public void processElement(Bean bean, Context context, Collector<Bean> collector) throws Exception {
                        long timerTime = bean.getTs() + 5000L;
                        if(sidState.value() == null || bean.getEventId().equals("launch") || bean.getEventId().equals("wake")) {
                            bean.setSplitSessionId(bean.getSessionId() + ":" +bean.getTs());
                            log.info("起始注册定时器，时间： " + timerTime);
                            timerState.update(timerTime);
                            context.timerService().registerEventTimeTimer(timerTime);
                        }else if(bean.getEventId().equals("back") || bean.getEventId().equals("close")){
                            log.info("结束删除定时器，时间： " + timerState.value());
                            context.timerService().deleteEventTimeTimer(timerState.value());
                        }else if(bean.getEventId().equals("pload")) {
                            bean.setSplitSessionId(sidState.value().getSplitSessionId());

                            if(pageLoadTimeState.value() != null ){
                                bean.setPageId(pageLoadTimeState.value().f0);
                                bean.setPageLoadTime(pageLoadTimeState.value().f1);
                                collector.collect(bean);
                            }

                            pageLoadTimeState.update(Tuple2.of(bean.getProperties().get("pageId"),bean.getTs()));

                        }else{
                            context.timerService().deleteEventTimeTimer(timerState.value());
                            log.info("中间删除定时器，时间： " + timerState.value());
                            log.info("中间注册定时器，时间： " + timerTime);
                            timerState.update(timerTime);
                            context.timerService().registerEventTimeTimer(timerTime);
                            bean.setSplitSessionId(sidState.value().getSplitSessionId());
                        }

                        bean.setPageId(pageLoadTimeState.value()==null?null:pageLoadTimeState.value().f0);
                        bean.setPageLoadTime(pageLoadTimeState.value()==null?null:pageLoadTimeState.value().f1);

                        sidState.update(bean);
                        collector.collect(bean);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Bean> out) throws Exception {

                        Long currentKey = ctx.getCurrentKey();
                        log.info("定时器被调用，当前key为： " + currentKey);

                        Bean bean = sidState.value();
                        bean.setTs(timerState.value());
                        bean.setEventId("flag");
                        bean.setPageId(pageLoadTimeState.value().f0);
                        bean.setPageLoadTime(pageLoadTimeState.value().f1);
                        out.collect(bean);

                        if(timerState.value() - sidState.value().getTs() < 60000){
                            Long nextTimerTime = timerState.value() + 5000;
                            log.info("再次注册定时器，时间为：" + nextTimerTime);
                            ctx.timerService().registerEventTimeTimer(nextTimerTime);
                            timerState.update(nextTimerTime);
                        }

                    }
                });
        // stream.print();

        tenv.createTemporaryView("t",stream, Schema.newBuilder()
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3),"rowtime")
                .watermark("rowtime","rowtime")
                .build());
        //tenv.executeSql("create view t2 as select guid,eventId,rowtime  from t");
        //tenv.executeSql("desc t2").print();

        tenv.executeSql("select * from t").print();

        /*tenv.executeSql("select\n" +
                "  guid,\n" +
                "  sessionId,\n" +
                " splitSessionId, \n" +
                "  max(ts) - min(ts) as timelong\n" +
                "from t\n" +
                "group by guid,sessionId,splitSessionId").print();*/

        tenv.executeSql("with tmp as (            " +
                "select                                    " +
                "  guid,                                   " +
                "  sessionId,                              " +
                "  splitSessionId,                         " +
                "  max(ts) - min(ts) as timelong           " +
                "from t                                    " +
                "group by guid,sessionId,splitSessionId    " +
                ")                                         " +
                "select                                    " +
                "  guid,                                   " +
                "  sessionId,                              " +
                "  sum(timelong) as session_time           " +
                "from tmp                                  " +
                "group by guid,sessionId")/*.print()*/;

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bean{
        private long guid;
        private String province;
        private String city;
        private String sessionId;
        private String splitSessionId;
        private long ts;
        private String eventId;
        private String pageId;
        private Long pageLoadTime;
        private Map<String,String> properties;
    }
}
