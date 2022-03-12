package cn.doitedu.flink.exercise;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pojos.EventLog;

import java.util.Properties;

public class EventLogGenerator {
    public static void main(String[] args) throws InterruptedException {
        String[] channels = {"app","wxapp","h5","web"};
        String[] events = {"pageview","share","addcart","payment","subscribe","videoplay"};


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG,"1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        // {"guid":1,"eventId":"addCart","channel":"app","timeStamp":1646784001000,"stayLong":200}
        boolean flag = true;
        while(flag) {
            EventLog eventLog = new EventLog();

            eventLog.setGuid(RandomUtils.nextInt(1,3));
            eventLog.setChannel(channels[RandomUtils.nextInt(0,channels.length)]);
            eventLog.setEventId(events[RandomUtils.nextInt(0,events.length)]);
            eventLog.setStayLong(RandomUtils.nextLong(100,2000));
            eventLog.setTimeStamp(System.currentTimeMillis());

            String json = JSON.toJSONString(eventLog);

            ProducerRecord<String, String> record = new ProducerRecord<>("tpc0312", json);
            producer.send(record);

            Thread.sleep(200);
        }

        producer.flush();
    }
}
