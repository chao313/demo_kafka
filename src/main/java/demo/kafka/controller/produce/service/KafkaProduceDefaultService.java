package demo.kafka.controller.produce.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProduceDefaultService<K, V> extends KafkaProduceService {


    /**
     * 构造函数(直接注入 kafkaProducer)
     */
    public static <K, V> KafkaProduceDefaultService<K, V> getInstance(KafkaProducer kafkaProducer) {
        return new KafkaProduceDefaultService(kafkaProducer);
    }


    private KafkaProduceDefaultService(KafkaProducer kafkaProducer) {
        super(kafkaProducer);
    }

    /**
     * 这里不做支持
     *
     * @param record
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Override
    public void sendProducerRecord(ProducerRecord record) throws ExecutionException, InterruptedException {
        throw new UnsupportedOperationException();
    }
}
