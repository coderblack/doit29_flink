package cn.doitedu.flink.sqls;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/5
 **/
@Slf4j
public class AnalyseProcessFunc extends KeyedProcessFunction<Long, Traffic.Bean, Traffic.Bean> {
    // guid,sessionId,splitSessionId,eventId,pageId,pageLoadTime,ts

    // 状态1： 当前bean
    ValueState<Traffic.Bean> beanState;
    // 状态2： 当前页面及加载时间
    ValueState<Tuple2<String, Long>> pageState;
    // 状态3： 定时器时间戳
    ValueState<Long> timerState;

    @Override
    public void open(Configuration configuration) throws Exception {
        beanState = getRuntimeContext().getState(new ValueStateDescriptor<Traffic.Bean>("bean", Traffic.Bean.class));
        pageState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, Long>>("page", Types.TUPLE(Types.STRING, Types.LONG)));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
    }

    @Override
    public void processElement(Traffic.Bean bean, Context context, Collector<Traffic.Bean> collector) throws Exception {
        // 删除之前的定时器（如果存在），并注册定时器
        if (timerState.value() != null) context.timerService().deleteEventTimeTimer(timerState.value());

        if (!"pushback".equals(bean.getEventId()) && !"close".equals(bean.getEventId())) {
            long newTimerTime = bean.getTs() + 5000;
            // 只要不是 pushback 或 close，则注册定时器
            context.timerService().registerEventTimeTimer(newTimerTime);
            timerState.update(newTimerTime);
            log.warn("注册了定时器：" + newTimerTime);

            // 如果遇到 “启动”、“唤醒”或 beanState为空，则需要更新splitSessionId,如果pageState不空，还需要更新pageloadTIme
            if ("launch".equals(bean.getEventId()) || "wake".equals(bean.getEventId()) || beanState.value() == null) {
                bean.setSplitSessionId(bean.getSessionId() + ":" + bean.getTs());
                beanState.update(bean);
                Tuple2<String, Long> pageStateValue = pageState.value();
                if(pageStateValue != null ) pageState.update(Tuple2.of(pageStateValue.f0,bean.getTs()));
            }

            // 如果遇到  pageload 事件，则需要更新pageState
            if ("pload".equals(bean.getEventId())) {
                if (pageState.value() != null) {
                    bean.setPageId(pageState.value().f0);
                    bean.setPageLoadTime(pageState.value().f1);
                    collector.collect(bean);
                }
                pageState.update(Tuple2.of(bean.getProperties().get("pageId"), bean.getTs()));
            }
        }

        bean.setSplitSessionId(beanState.value().getSplitSessionId());
        bean.setPageId(pageState.value() == null ? null : pageState.value().f0);
        bean.setPageLoadTime(pageState.value() == null ? null : pageState.value().f1);

        beanState.update(bean);
        collector.collect(bean);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Traffic.Bean> out) throws Exception {
        Traffic.Bean bean = beanState.value();
        bean.setEventId("");
        bean.setTs(timestamp);

        out.collect(bean);

        if(timerState.value() - bean.getTs() < 60000){
            Long nextTimerTime = timerState.value() + 5000;
            ctx.timerService().registerEventTimeTimer(nextTimerTime);
            log.warn("定时器又自注册了新定时器： " + nextTimerTime);
            timerState.update(nextTimerTime);
        }


    }
}
