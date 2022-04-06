package eagle.functions;

import eagle.pojo.EventBean;
import eagle.pojo.TrafficBean;
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
 * @date 2022/4/6
 **/
public class TrafficAnalyseFunc extends KeyedProcessFunction<Long, EventBean, TrafficBean> {

    ValueState<Tuple2<String, Long>> pageState;
    ValueState<Long> timerState;
    ValueState<TrafficBean> beanState;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 开辟一个状态，记录： 当前所在的页面，及其加载时间
        pageState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, Long>>("pageState", Types.TUPLE(Types.STRING, Types.LONG)));

        // 开辟一个状态，记录：最新的定时器时间
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

        // 开辟一个状态，记录：最后一条eventBean
        beanState = getRuntimeContext().getState(new ValueStateDescriptor<TrafficBean>("beanState", TrafficBean.class));
    }

    @Override
    public void processElement(EventBean eventBean, Context context, Collector<TrafficBean> collector) throws Exception {

        if(eventBean.getEventid().equals("applaunch") || eventBean.getEventid().equals("wakeup") || beanState.value()==null){
            // 生成 splitSessionId，并更新到 beanState
            TrafficBean trafficBean = new TrafficBean(eventBean.getGuid()
                    , eventBean.getSessionid()
                    , eventBean.getSessionid() + ":" + eventBean.getTimestamp()  // 生成split会话id
                    , eventBean.getEventid()
                    , eventBean.getTimestamp()
                    , beanState.value() == null ? null : beanState.value().getPageId()  // 如果是wakeup，则当前页面，就是唤醒前所在的页面（就在beanState的数据中）
                    , eventBean.getTimestamp()
                    , eventBean.getProvince()
                    , eventBean.getCity()
                    , eventBean.getRegion()
                    , eventBean.getDevicetype()
            );
            // 更新到beanState中
            beanState.update(trafficBean);
        }

        // 如果是一个页面加载事件，则需要输出一条“虚的插值数据”来对上一个页面进行封闭，并更新“当前页面”状态
        else  if(eventBean.getEventid().equals("pageload")){
            TrafficBean preBean = beanState.value();
            preBean.setEventId("flag");
            preBean.setTs(eventBean.getTimestamp());  // 利用状态中记录的上个事件bean，修改一下时间戳，来生成一条插值数据
            collector.collect(preBean);

            // 更新 “当前页面”状态
            pageState.update(Tuple2.of(eventBean.getProperties().get("pageId"), eventBean.getTimestamp()));

            // 更新 ”beanState“中上一条bean的eventid为当前最新的事件id
            preBean.setEventId(eventBean.getEventid());
            // 更新 "beanState"中上一条bean的页面id为当前最新的页面id
            preBean.setPageId(eventBean.getProperties().get("pageId"));
            // 更新 "beanState" 中上一条bean的页面加载时间为当前时间
            preBean.setPageLoadTime(eventBean.getTimestamp());

            beanState.update(preBean);
        }
        else {

            TrafficBean preBean = beanState.value();
            // 更新 ”beanState“中上一条bean的eventid为当前最新的事件id
            preBean.setEventId(eventBean.getEventid());
            // 更新 "beanState" 中行为时间为当前时间
            preBean.setTs(eventBean.getTimestamp());
        }


        collector.collect(beanState.value());

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TrafficBean> out) throws Exception {


    }
}
