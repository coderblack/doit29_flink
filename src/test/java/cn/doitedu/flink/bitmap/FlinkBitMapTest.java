//package cn.doitedu.flink.bitmap;
//
//import com.google.common.hash.BloomFilter;
//import com.google.common.hash.Funnels;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.roaringbitmap.RoaringBitmap;
//
//public class FlinkBitMapTest {
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> stream = env.socketTextStream("doit01", 9999);
//        stream
//                .keyBy(s -> s)
//                .map(new RichMapFunction<String, String>() {
//                    RoaringBitmap bm;
//
//                    BloomFilter<CharSequence> bf;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        // 构造一个bitmap
//                        bm = new RoaringBitmap();
//
//                        // 构造一个布隆过滤器
//                        bf = BloomFilter.create(Funnels.stringFunnel(), 100000000, 0.01);
//
//                    }
//
//                    @Override
//                    public String map(String deviceId) throws Exception {
//
//                        /**
//                         * 用bitmap做
//                         */
//                        // 先判断本次的deviceId是否存在于历史记录中
//                /* boolean exists = bm.contains(deviceId.hashCode() & Integer.MAX_VALUE);
//                if(!exists) {
//                    bm.add(deviceId.hashCode() & Integer.MAX_VALUE);  // 可能会产生hash冲突
//                }*/
//
//                        /**
//                         * 用布隆过滤器做
//                         */
//                        boolean exists = bf.mightContain(deviceId);
//                        if (!exists) {
//                            bf.put(deviceId);
//                        }
//
//                        return deviceId + "," + (exists ? "老用户" : "新用户");
//                    }
//                }).print();
//
//
//        env.execute();
//
//    }
//
//
//}
