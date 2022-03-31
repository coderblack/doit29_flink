package eagle.functions;

import eagle.pojo.EventBean;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 如果我们要存储的状态数据，不用跟任何业务 key强 绑定，则使用算子状态更合适
 */
public class DimensionJoinFunction extends ProcessFunction<EventBean, EventBean> implements CheckpointedFunction {

    @Override
    public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context context, Collector<EventBean> collector) throws Exception {



    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {



    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        OperatorStateStore operatorStateStore = functionInitializationContext.getOperatorStateStore();
        ListState<Tuple4<String, String, String, String>> geoState = operatorStateStore.getListState(new ListStateDescriptor<Tuple4<String, String, String, String>>("geo", TypeInformation.of(new TypeHint<Tuple4<String, String, String, String>>() {
        })));
    }

}
