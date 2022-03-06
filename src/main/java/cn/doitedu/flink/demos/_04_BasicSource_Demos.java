package cn.doitedu.flink.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

public class _04_BasicSource_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 通过一个本地集合（序列）来构造一个数据流
        DataStreamSource<Long> source1 = env.fromSequence(1, 1000);
        source1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 1;
            }
        })/*.print()*/;

        // 1. 通过一个本地集合（List）来构造一个数据流
        DataStreamSource<String> source2 = env.fromCollection(Arrays.asList("a", "b", "c", "d"));
        source2.map(String::toUpperCase)/*.print()*/;

        // 2.封装好的读文本文件的api（里面的InputFormat是写死的：TextInputFormat
        env.readTextFile("data/wc");

        // 3.底层自己指定inputFormat的读取文件数据的api
        TextInputFormat format = new TextInputFormat(new Path("data/wc/"));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        //TypeInformation<String> stringType = TypeInformation.of(String.class);
        format.setCharsetName("UTF-8");

        // FileProcessingMode.PROCESS_CONTINUOUSLY 每监听到文件中有新增内容，就会将整个文件重新读取计算一遍
        DataStreamSource<String> source3 = env.readFile(format, "data/wc/", FileProcessingMode.PROCESS_CONTINUOUSLY, 100, typeInfo);
        /*source3.print();*/

        // 4.通过socket构造流
        DataStreamSource<String> source4 = env.socketTextStream("localhost", 9099);


        // 5.通过addSource(SourceFunction实现）/ fromSource(Source的实现类）  来添加源
        /*
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("flk001")
                .setTopics("topic-flk")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<String> source5 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        source5.print();*/



        // 添加自定义的数据源
        env.addSource(new MySourceFunction())
                // TODO 练习题：过滤掉所有年龄小于20岁的访客，然后实时统计各访客的年龄最大值，年龄最小值，年龄平均值，到目前为止的访客总数（不用对id去重）
                .print();



        env.execute();
    }
}

class MySourceFunction implements SourceFunction<String>{
    boolean flag = true;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(flag){
            int id = RandomUtils.nextInt(1, 1000);
            String name = RandomStringUtils.randomAlphabetic(3);
            int age = RandomUtils.nextInt(18, 38);
            String gender = RandomUtils.nextInt(1,8) % 2 ==0 ? "male":"female";

            ctx.collect(String.format("%d,%s,%d,%s",id,name,age,gender));
            Thread.sleep(RandomUtils.nextInt(100,2000));
        }
    }

    @Override
    public void cancel() {
       flag = false;
    }
}
