package demo.kafka.controller.consume.service.base;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerService<K, V> {

    protected KafkaConsumer<K, V> consumer;

    protected ConsumerService(KafkaConsumer<K, V> consumer) {
        this.consumer = consumer;
    }

    private ConsumerService() {
    }

    public KafkaConsumer<K, V> getConsumer() {
        return consumer;
    }
}
