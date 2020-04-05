package demo.kafka.controller.produce.service;

import demo.kafka.controller.produce.vo.RecordMetadataResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * 发送就忘记 - 不关心是否发生成功
 *
 * @return
 */

@Slf4j
public class KafkaProduceSendForgetService<K, V> extends KafkaProduceService {

    public KafkaProducer getKafkaProducer() {
        return super.kafkaProducer;
    }

    /**
     * 底层()
     */
    public void sendForget(ProducerRecord<K, V> producerRecord) {
        super.kafkaProducer.send(producerRecord);
    }

    /**
     * 这个是最全的 全部的的发送都要调用这个
     *
     * @param topic     指定 topic
     * @param partition 指定 分区
     * @param timestamp 指定 时间戳
     * @param key       指定 key
     * @param value     指定 value
     * @param headers   指定 headers
     */

    public void sendForget(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, timestamp, key, value, headers);
        this.sendForget(record);
    }

    /**
     * 这里忽略了 headers
     */
    public void sendForget(String topic, Integer partition, Long timestamp, K key, V value) {
        this.sendForget(topic, partition, timestamp, key, value, null);
    }

    /**
     * 这里忽略了 headers 和 timestamp
     */
    public void sendForget(String topic, Integer partition, K key, V value) {
        this.sendForget(topic, partition, null, key, value, null);
    }

    /**
     * 这里忽略了 headers 和 timestamp 和 partition
     */
    public void sendForget(String topic, K key, V value) {
        this.sendForget(topic, null, null, key, value, null);
    }

    /**
     * 这个是最少的需要 topic 和 value
     *
     * @param topic
     * @param value
     */
    public void sendForget(String topic, V value) {
        this.sendForget(topic, null, null, null, value, null);
    }


    @Override
    public void sendProducerRecord(ProducerRecord record) throws ExecutionException, InterruptedException {
        this.sendForget(record);
    }
}
