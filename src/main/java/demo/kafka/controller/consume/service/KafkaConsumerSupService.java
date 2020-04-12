package demo.kafka.controller.consume.service;

import demo.kafka.util.MapUtil;
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
 * 增强版(使用组合模式)
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class KafkaConsumerSupService<K, V> {

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> KafkaConsumerSupService<K, V> getInstance(KafkaConsumerService kvKafkaConsumerService) {
        return new KafkaConsumerSupService(kvKafkaConsumerService);
    }

    private KafkaConsumerService<K, V> kvKafkaConsumerService;


    private KafkaConsumerSupService() {
    }

    private KafkaConsumerSupService(KafkaConsumerService<K, V> kvKafkaConsumerService) {
        this.kvKafkaConsumerService = kvKafkaConsumerService;
    }


    /**
     * 普通的监听函数
     */
    public void listener(Collection<String> topics, Consumer<ConsumerRecord<K, V>> consumer) {
        this.kvKafkaConsumerService.subscribe(topics);
        while (true) {
            ConsumerRecords<K, V> records = kvKafkaConsumerService.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                consumer.accept(record);
            });
        }
    }

    /**
     * 普通的监听函数(只一次)
     */
    public void listenerOnce(Collection<String> topics, Consumer<ConsumerRecord<K, V>> consumer) {
        this.kvKafkaConsumerService.subscribe(topics);
        ConsumerRecords<K, V> records;
        records = this.kvKafkaConsumerService.poll(Duration.ofMillis(1000));
        records.forEach(record -> {
            consumer.accept(record);
        });
        log.info("尝试获取一批数据...:{}", records.count());
        this.kvKafkaConsumerService.wakeup();
    }

    /**
     * 根据 Topic 来获取 Partition
     *
     * @return
     */
    public Collection<PartitionInfo> getPartitionInfosByTopic(String topic) {
        return this.kvKafkaConsumerService.partitionsFor(topic);
    }


    /**
     * 根据 Topic 来获取 TopicPartition
     *
     * @return
     */
    public Collection<TopicPartition> getTopicPartitionsByTopic(String topic) {
        List<PartitionInfo> partitionInfos = this.kvKafkaConsumerService.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitionInfos.forEach(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        });
        return topicPartitions;
    }

    /**
     * 获取最新的 record (每个分区的)
     */
    public void getLastRecordEachPartition(String topic, Consumer<ConsumerRecord<K, V>> consumer) {
        List<PartitionInfo> partitionInfos = this.kvKafkaConsumerService.partitionsFor(topic);

        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitionInfos.forEach(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        });


        this.kvKafkaConsumerService.assign(topicPartitions);

        Map<TopicPartition, Long> topicPartitionLongMap = this.kvKafkaConsumerService.endOffsets(topicPartitions);

        topicPartitionLongMap.forEach((topicPartition, offsetSize) -> {
            this.kvKafkaConsumerService.seek(topicPartition, offsetSize > 0 ? offsetSize - 1 : 0);
        });

        ConsumerRecords<K, V> records;
        records = this.kvKafkaConsumerService.poll(100);
        while (records.isEmpty()) {
            records = this.kvKafkaConsumerService.poll(100);
        }
        records.forEach(record -> {
            consumer.accept(record);
        });
        log.info("尝试获取一批数据...:{}", records.count());
        this.kvKafkaConsumerService.close();
    }


}
