package demo.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Service
public class ProduceService {

    public static KafkaProducer kafkaProducer;

    public void xx() {
        ProducerRecord record = new ProducerRecord<String, String>("", "");
        kafkaProducer.send(record);
    }
}
