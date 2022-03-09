package cn.doitedu.flink.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import pojos.EventLog;

public class MyCountTrigger extends Trigger<EventLog, GlobalWindow> {
    /**
     * 每来一条数据调用一次该方法，以决定是否需要触发窗口计算
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    int maxCount = 2;
    int count = 0;
    @Override
    public TriggerResult onElement(EventLog element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        count++;
        if(count >= maxCount){    // 每新增2条数据触发一次
            count = 0;
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * 用于处理时间条件触发，当满足什么时间的时候去触发
     * @param time
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * 用于时间时间条件触发
     * @param time
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("窗口被清除...... ");

    }
}
