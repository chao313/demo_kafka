package demo.kafka.controller.consume;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumeController {

    public void xx() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broekr1:9092,broker2:9092");
        props.put("group.id", "xxxx");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    }
}
