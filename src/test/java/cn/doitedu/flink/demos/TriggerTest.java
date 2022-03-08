package cn.doitedu.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/3/8
 **/
public class TriggerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9099);

        source.windowAll(GlobalWindows.create())
                .trigger(new MyTrigger())
                .evictor(new MyEvictor(5))
                .apply(new AllWindowFunction<String, String, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<String> values, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        for (String value : values) {
                            sb.append(value).append(":");
                        }
                        out.collect("计算结果：" + sb.toString());
                    }
                })
                .print();

        env.execute();
    }
}


class MyTrigger extends Trigger<String, GlobalWindow> {

    @Override
    public TriggerResult onElement(String element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        if (element.startsWith("x")) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        System.out.println("窗口被调用  clear   ....");

    }
}

class MyEvictor implements Evictor<String,GlobalWindow>{
    int maxSize;
    public MyEvictor(int maxSize){
        this.maxSize = maxSize;
    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        int deleted = 0;
        StringBuilder sb1 = new StringBuilder();
        for (Iterator<TimestampedValue<String>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<String> next = iterator.next();
            sb1.append("_").append(next.getValue());
        }
        System.out.println("移除之前的窗口数据是： " + sb1.toString());

        for (Iterator<TimestampedValue<String>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<String> next = iterator.next();
            if(deleted>size-maxSize){
                break;
            }
            iterator.remove();
            System.out.println("移除掉了: " +next.getValue());
            deleted++;
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
        StringBuilder sb1 = new StringBuilder();
        for (Iterator<TimestampedValue<String>> iterator = elements.iterator();
             iterator.hasNext(); ) {
            TimestampedValue<String> next = iterator.next();
            sb1.append("_").append(next.getValue());
        }
        System.out.println("evictAfter 被调用...... ,此时的窗口数据为：  " + sb1);
    }
}