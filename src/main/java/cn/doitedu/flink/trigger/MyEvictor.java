package cn.doitedu.flink.trigger;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import pojos.EventLog;

import java.util.Iterator;

public class MyEvictor implements Evictor<EventLog, GlobalWindow> {

    /**
     * 窗口计算触发前调用该方法（可以用来在触发计算前剔除一些元素）
     * @param elements  此刻，窗口中的所有元素的迭代器
     * @param size  此刻，窗口中的元素的总个数
     * @param window  当前窗口的元信息
     * @param evictorContext
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<EventLog>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        if(size > 4 ){
            int evictedCount = 0;
            for(Iterator<TimestampedValue<EventLog>> iterator = elements.iterator();iterator.hasNext();){
                TimestampedValue<EventLog> next = iterator.next();
                iterator.remove();
                evictedCount++;
                if(size - evictedCount <= 4) break;
            }
        }
    }

    /**
     * 窗口计算触发后调用该方法（可以用来在窗口计算完毕后剔除一些元素）
     * @param elements
     * @param size
     * @param window
     * @param evictorContext
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<EventLog>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        System.out.println(" evictAfter 被调用了.....");

    }
}
