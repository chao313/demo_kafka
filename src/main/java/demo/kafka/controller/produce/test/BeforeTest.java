package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;

import java.util.Properties;

public class BeforeTest {

    @BeforeAll
    public static void BeforeAll() {
        Properties kafkaProps = new Properties(); //新建一个Properties对象
        kafkaProps.put("bootstrap.servers", Bootstrap.HONE.getIp());
        kafkaProps.put("retries", "1");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key准备是String -> 使用了内置的StringSerializer
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value准备是String -> 使用了内置的StringSerializer
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(kafkaProps);//创建生产者
        KafkaProduceService.kafkaProducer = kafkaProducer;
    }
}
