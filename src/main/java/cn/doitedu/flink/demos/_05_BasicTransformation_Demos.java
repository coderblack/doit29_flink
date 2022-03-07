package cn.doitedu.flink.demos;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class _05_BasicTransformation_Demos {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromCollection(Arrays.asList("a,1,28_e,5,32", "b,2,33", "c,3,38_d,4,36"));

        // flatmap
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split("_")) {
                    out.collect(s);
                }
            }
        });

        // 如果算子的函数，使用lambda表达式形式传入，则需要显式声明函数中的泛型参数类型
        SingleOutputStreamOperator<String> flatMaped = source.flatMap(
                (String value, Collector<String> out) -> {
                    Arrays.stream(value.split("_")).forEach(out::collect);
                }).returns(String.class);
        /*.returns(String.class)*/  // 如果泛型参数是一个基本类型，则直接  .returns(String.class)
        /*.returns(new TypeHint<String>() {})*/  // 如果泛型参数是一个泛型类，可以用TypeHint的空实现来表达
        /*.returns(Types.TUPLE(Types.STRING,Types.INT))*/
        /*.returns(TypeInformation.of(new TypeHint<String>() {}))*/

        flatMaped.print();


        // map  把字符串数据 "c,3,38" 变成  Person对象数据
        DataStream<Person> personStream = flatMaped.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Person(arr[0], Integer.parseInt(arr[1]), Integer.parseInt(arr[2]));
            }
        });

        // filter  过滤掉age<30的人
        DataStream<Person> filtered = personStream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.getAge() >= 30;
            }
        });

        filtered.print();


        /**
         * 随堂小练习
         *
         */
        DataStreamSource<String> source2 = env
                .fromCollection(
                        Arrays.asList(
                                "{\"name\":\"aaa\",\"age\":18,\"id\":1}",
                                "{\"name\":\"bbb\",\"age\":28,\"id\":2}",
                                "{\"name\":\"ccc\",\"age\":38,\"id\":3}",
                                "{\"name\":\"ddd\",\"age\":48,\"id\":4}" )
                );

        // TODO  对上面的数据流转换成Person数据流,并过滤掉所有年龄<20的人，打印输出

        env.execute();

    }
}

class Person implements Serializable {
    private String name;
    private int id;
    private int age;

    public Person() {
    }

    public Person(String name, int id, int age) {
        this.name = name;
        this.id = id;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", age=" + age +
                '}';
    }
}