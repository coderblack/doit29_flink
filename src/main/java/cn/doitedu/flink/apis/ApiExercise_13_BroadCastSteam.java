package cn.doitedu.flink.apis;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import cn.doitedu.flink.pojos.StuInfo;
import cn.doitedu.flink.pojos.Student;

/**
 * 广播流的使用示例
 */
public class ApiExercise_13_BroadCastSteam {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // id,name,gender,score
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9098);

        // id:phone,city
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9099);


        SingleOutputStreamOperator<Student> s1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new Student(Integer.parseInt(arr[0]), arr[1], arr[2], Double.parseDouble(arr[3]));
        }).returns(Student.class);


        SingleOutputStreamOperator<StuInfo> s2 = source2.map(s -> {
            String[] arr = s.split(":");
            return new StuInfo(Integer.parseInt(arr[0]), arr[1], arr[2]);
        });

        // 将s2流广播出去
        MapStateDescriptor<Integer, StuInfo> stateDescriptor = new MapStateDescriptor<>("info", Integer.class, StuInfo.class);
        BroadcastStream<StuInfo> stuInfoBroadcastStream = s2.broadcast(stateDescriptor);

        // 拿着学生事件流，connect这个学生信息广播流
        s1.connect(stuInfoBroadcastStream)
                .process(new BroadcastProcessFunction<Student, StuInfo, String>() {

                    BroadcastState<Integer, StuInfo> broadcastState;

                    /**
                     * 处理主流中的一条数据
                     * @param stu
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Student stu, BroadcastProcessFunction<Student, StuInfo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        StuInfo info;
                        if (broadcastState != null && (info = broadcastState.get(stu.getId())) != null) {
                            out.collect(String.format("%d,%s,%s,%.2f,%d,%s,%s", stu.getId(), stu.getName(), stu.getGender(), stu.getScore(), info.getId(), info.getCity(), info.getPhone()));
                        } else {
                            out.collect(String.format("%d,%s,%s,%.2f,%d,%s,%s", stu.getId(), stu.getName(), stu.getGender(), stu.getScore(), null, null, null));
                        }
                    }

                    /**
                     * 处理广播流中的一条数据
                     * @param stuInfo
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(StuInfo stuInfo, BroadcastProcessFunction<Student, StuInfo, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取广播状态的操作对象
                        if (broadcastState == null) {
                            broadcastState = ctx.getBroadcastState(stateDescriptor);
                        }
                        // 将当前收到的stuInfo数据放入广播状态
                        broadcastState.put(stuInfo.getId(), stuInfo);

                    }
                }).print();


        env.execute();

    }
}
