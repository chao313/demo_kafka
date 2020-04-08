package demo.kafka.controller.consume.service;

public class ConsumerService<K, V> {

    KafkaConsumerService<K, V> kafkaConsumerService;

    ConsumerService(KafkaConsumerService<K, V> kafkaConsumerService) {
        this.kafkaConsumerService = kafkaConsumerService;
    }

    private ConsumerService() {
    }
}
