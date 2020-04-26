package demo.kafka.controller.consume.service;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerService<K, V> {

    KafkaConsumer<K, V> consumer;

    ConsumerService(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    private ConsumerService() {
    }

    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }
}
