package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;

import java.util.Properties;

public class BeforeTest {

    @BeforeAll
    public static void BeforeAll() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Bootstrap.HONE.getIp());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "common_imp_db_test1");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        KafkaConsumerService.kafkaConsumer = kafkaConsumer;
    }
}
