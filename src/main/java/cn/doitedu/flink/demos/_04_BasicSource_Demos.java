package cn.doitedu.flink.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;

/**
 * 各类source组件的演示
 */
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
        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition topicPartition1 = new TopicPartition("doitedu", 0);
        startingOffsets.put(topicPartition1,3L);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doit01:9092,doit02:9092,doit03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("gp001")
                .setTopics("doitedu")
                // OffsetsInitializer.earliest() 从最小偏移量开始消费
                // OffsetsInitializer.earliest() 从最新偏移量开始消费
                // OffsetsInitializer.offsets(Map<TopicPartition,offset>)  从指定的偏移量开始消费

                // OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 从上次记录的消费位置开始接着消费（如果没有之前记录好的偏移量，则将消费起始位置重置为latest）
                // 要求开启flink的checkpoint机制，flink内部是把consumer的消费位移记录在自己的checkpoint存储中；
                // 如果没有开启checkpoint，也可以继续使用kafka的原生偏移量自动提交机制，需要配置如下两个参数
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"100")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .build();
        // 使用fromSource添加到执行计划中
        env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kfk")/*.print()*/;


        // 添加自定义的数据源
        DataStreamSource<String> source = env.addSource(new MyParallelSourceFunction()).setParallelism(2);
        // TODO 练习题：过滤掉所有年龄小于20岁的访客，然后实时统计各访客的年龄最大值，年龄最小值，年龄平均值，到目前为止的访客总数（不用对id去重）
        /*.print()*/


        env.execute();
    }
}

class MySourceFunction implements SourceFunction<String> {
    boolean flag = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (flag) {
            int id = RandomUtils.nextInt(1, 1000);
            String name = RandomStringUtils.randomAlphabetic(3);
            int age = RandomUtils.nextInt(18, 38);
            String gender = RandomUtils.nextInt(1, 8) % 2 == 0 ? "male" : "female";

            ctx.collect(String.format("%d,%s,%d,%s", id, name, age, gender));
            Thread.sleep(RandomUtils.nextInt(100, 2000));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

class MyRichSourceFunction extends RichSourceFunction<String> {
    boolean flag = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (flag) {
            int id = RandomUtils.nextInt(1, 1000);
            String name = RandomStringUtils.randomAlphabetic(3);
            int age = RandomUtils.nextInt(18, 38);
            String gender = RandomUtils.nextInt(1, 8) % 2 == 0 ? "male" : "female";

            ctx.collect(String.format("%d,%s,%d,%s", id, name, age, gender));
            Thread.sleep(RandomUtils.nextInt(100, 2000));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }



    @Override
    public void cancel() {
        flag = false;
    }
}


/**
 * 并行的source算子，并行度允许设置为大于1
 */
class MyParallelSourceFunction implements ParallelSourceFunction<String> {
    boolean flag = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (flag) {
            int id = RandomUtils.nextInt(1, 1000);
            String name = RandomStringUtils.randomAlphabetic(3);
            int age = RandomUtils.nextInt(18, 38);
            String gender = RandomUtils.nextInt(1, 8) % 2 == 0 ? "male" : "female";

            ctx.collect(String.format("%d,%s,%d,%s", id, name, age, gender));
            Thread.sleep(RandomUtils.nextInt(100, 2000));
        }
    }


    @Override
    public void cancel() {
        flag = false;
    }
}

/**
 * 并行的source算子，并行度允许设置为大于1
 */
class MyRichParallelSourceFunction extends RichParallelSourceFunction<String> {
    boolean flag = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }




    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (flag) {
            int id = RandomUtils.nextInt(1, 1000);
            String name = RandomStringUtils.randomAlphabetic(3);
            int age = RandomUtils.nextInt(18, 38);
            String gender = RandomUtils.nextInt(1, 8) % 2 == 0 ? "male" : "female";

            ctx.collect(String.format("%d,%s,%d,%s", id, name, age, gender));
            Thread.sleep(RandomUtils.nextInt(100, 2000));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void cancel() {
        flag = false;
    }
}



