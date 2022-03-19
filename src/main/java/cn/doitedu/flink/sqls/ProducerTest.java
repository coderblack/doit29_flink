package cn.doitedu.flink.sqls;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {


        Properties props = new Properties();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


    }
}
