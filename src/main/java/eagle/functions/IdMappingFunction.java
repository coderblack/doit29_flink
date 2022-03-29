package eagle.functions;

import eagle.pojo.AccountIdMapBean;
import eagle.pojo.DeviceIdMapBean;
import eagle.pojo.EventBean;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class IdMappingFunction extends KeyedProcessFunction<String, EventBean, EventBean> {

    MapState<String, AccountIdMapBean> accountIdMapState;
    MapState<String, DeviceIdMapBean> deviceIdMapState;
    @Override
    public void open(Configuration parameters) throws Exception {

        // 构造一个用于存储  账号->guid 信息的本地状态
        MapStateDescriptor<String, AccountIdMapBean> accountIdMapStateDescriptor = new MapStateDescriptor<>("account_idmp_state", String.class, AccountIdMapBean.class);
        accountIdMapState = getRuntimeContext().getMapState(accountIdMapStateDescriptor);

        // 构造一个用于存储  设备号->guid 信息的本地状态
        MapStateDescriptor<String, DeviceIdMapBean> deviceIdMapStateDescriptor = new MapStateDescriptor<>("device_idmp_state", String.class, DeviceIdMapBean.class);
        deviceIdMapState = getRuntimeContext().getMapState(deviceIdMapStateDescriptor);

    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {


        // 判断是否有账号，如果有，则用账号在state中查询信息，如果state中没有，则去mysql中查询注册信息  ，并且查完后，放入state


        // 如果没有账号，则用deviceid去state中查询，如果state中没有，则去hbase查询设备所绑定的账号，然后再查mysql，并且查完后，放入state


        // 如果查不到所绑定的账号，则去  deviceid临时guid表查询


        // 如果还查不到，则请求 hbase的技术进行增1，得到guid，并将结果插入 deviceid临时guid表


    }
}