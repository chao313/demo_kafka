package demo.kafka.controller.consume.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * 增强版
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class KafkaConsumerSupService<K, V> extends KafkaConsumerService<K, V> {

    /**
     * 普通的监听函数
     */
    public void listener(Collection<String> topics, Consumer<ConsumerRecords<K, V>> consumer) {
        super.subscribe(topics);
        while (true) {
            ConsumerRecords<K, V> records = super.poll(100);
            consumer.accept(records);
        }
    }

}
