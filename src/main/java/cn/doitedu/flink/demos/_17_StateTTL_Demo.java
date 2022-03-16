package cn.doitedu.flink.demos;

import lombok.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

/**
 * 状态TTL测试demo
 * 需求背景：
 * 如果一个用户做了一个 D 事件，则进而去判断该用户在最近 10s内是否还做过 B 事件 且在后面有做过 H事件，如果满足，则输出一条营销消息
 */
public class _17_StateTTL_Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        // uid,eventid  变对象流
        SingleOutputStreamOperator<UserEvent> userEvents = source.map(s -> {
            String[] arr = s.split(",");
            return new UserEvent(Long.parseLong(arr[0]), arr[1]);
        }).returns(UserEvent.class);

        // 按用户分组处理
        userEvents.keyBy(e->e.getUid())
                .process(new KeyedProcessFunction<Long, UserEvent, String>() {

                    ListState<UserEvent> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // 构造一个ttl的参数配置对象
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                                .useProcessingTime()  // 默认是用eventTime语义，如果需要用processingTime语义，则需要显式指定
                                //.updateTtlOnReadAndWrite() // ttl计时的重置刷新策略：数据只要被读取或者被写更新，则将它的ttl计时重置
                                //.updateTtlOnCreateAndWrite() // ttl计时的重置刷新策略：数据被创建及被写入更新，就将它的ttl计时重置
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)   // 状态数据的可见性：如果状态中存在还未清理掉的但是已经超出ttl的数据，是否让用户程序可见
                                .build();

                        // 构造一个list状态的描述器
                        ListStateDescriptor<UserEvent> stateDescriptor =
                                new ListStateDescriptor<>("events", UserEvent.class);
                        // 为描述器设置启用ttl功能
                        stateDescriptor.enableTimeToLive(ttlConfig);

                        // 获取一个list状态实例
                        listState = getRuntimeContext().getListState(stateDescriptor);

                    }

                    @Override
                    public void processElement(UserEvent event, KeyedProcessFunction<Long, UserEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        Iterable<UserEvent> events = listState.get();
                        // 观察测试用，打印当前的状态信息
                        System.out.println("--------------------------------------");
                        Iterator<UserEvent> iterator = events.iterator();
                        while(iterator.hasNext()){
                            System.out.println(iterator.next());
                        }
                        System.out.println("--------------------------------------");

                        // 判断当前这条行为事件是否是触发条件事件：D
                        events = listState.get();
                        if("D".equals(event.getEventId())){
                            System.out.println("触发事件D到达...........");
                            // 就需要去状态中查询 ：最近10s内，是否满足场景中的条件 :  做过B ，后面有做过 H
                            boolean ifB = false;
                            boolean ifH = false;
                            for (UserEvent userEvent : events) {
                                if("B".equals(userEvent.getEventId())){
                                    System.out.println("触发事件D后续判断，存在了B...........");
                                    ifB = true;
                                }
                                if(ifB && "H".equals(userEvent.getEventId())){
                                    ifH = true;
                                    System.out.println("触发事件D后续判断，存在了H...........");
                                    break;
                                }
                            }

                            // 如果满足，则输出一条营销短信
                            if(ifH)  out.collect("营销短信");
                        }

                        // 将用户的本次行为，插入到状态中
                        listState.add(event);

                    }
                })
                .print();

        env.execute();

    }
}

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
class UserEvent implements Serializable {
    private long uid;
    private String eventId;

}