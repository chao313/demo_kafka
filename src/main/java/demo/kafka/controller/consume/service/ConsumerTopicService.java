package demo.kafka.controller.consume.service;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ConsumerTopicService<K, V> extends ConsumerService<K, V> {

    ConsumerTopicService(KafkaConsumerService kafkaConsumerService) {
        super(kafkaConsumerService);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerTopicService<K, V> getInstance(KafkaConsumerService kafkaConsumerService) {
        return new ConsumerTopicService(kafkaConsumerService);
    }

    /**
     * 根据 获取全部的 Topic 和Partitions
     */
    public Map<String, List<PartitionInfo>> getAllTopicAndPartitions() {
        return super.kafkaConsumerService.listTopics();

    }

    /**
     * 根据 topic 获取 partitions
     */
    public Collection<PartitionInfo> getPartitionsByTopic(String topic) {
        List<PartitionInfo> partitionInfos = super.kafkaConsumerService.partitionsFor(topic);
        return partitionInfos;

    }

    /**
     * 根据 partition 来获取下一个偏移量
     */
    public long getNextOffsetByTopicAndPartition(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        super.kafkaConsumerService.assign(Arrays.asList(topicPartition));
        return super.kafkaConsumerService.position(topicPartition);
    }
}
