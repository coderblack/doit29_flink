package cn.doitedu.flink.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;
import java.util.Iterator;


public class MyStateMapFunc implements MapFunction<String,String>, CheckpointedFunction {
    ListState<String> listState ;
    ArrayList<String> tmpList = new ArrayList<String>();

    @Override
    public String map(String value) throws Exception {
        if(value.equals("x")) throw new Exception("我挂一个给你看");

        StringBuilder sb = new StringBuilder();

        for (String s : tmpList) {
            sb.append(s);
        }
        sb.append(value);

        // 更新自己的临时状态
        synchronized ("ok") {
            if (tmpList.size() > 1) tmpList.remove(0);
            tmpList.add(value);
        }
        return sb.toString();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.addAll(tmpList);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
       listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("lst", String.class));

        Iterator<String> iterator = listState.get().iterator();
        while(iterator.hasNext()){
            tmpList.add(iterator.next());
        }
    }
}
