package demo.kafka.controller.consume.service;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerNoGroupService<K, V> extends ConsumerService<K, V> {

    ConsumerNoGroupService(KafkaConsumerService kafkaConsumerService) {
        super(kafkaConsumerService);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerNoGroupService<K, V> getInstance(KafkaConsumerService kafkaConsumerService) {
        return new ConsumerNoGroupService(kafkaConsumerService);
    }

    /**
     * 根据 获取全部的 Topic 和Partitions
     */
    public Map<String, List<PartitionInfo>> getAllTopicAndPartitions() {
        return super.kafkaConsumerService.listTopics();

    }

    /**
     * 获取全部的 topic
     */
    public Set<String> getAllTopics() {
        return super.kafkaConsumerService.listTopics().keySet();

    }

    /**
     * 根据 topic 获取 partitions
     */
    public Collection<PartitionInfo> getPartitionsByTopic(String topic) {
        List<PartitionInfo> partitionInfos = super.kafkaConsumerService.partitionsFor(topic);
        return partitionInfos;

    }


    /**
     * 根据 topic 来生成 TopicPartitions
     */
    public Collection<TopicPartition> getTopicPartitionsByTopic(String topic) {
        Collection<PartitionInfo> partitionInfos = this.getPartitionsByTopic(topic);

        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitionInfos.forEach(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        });
        return topicPartitions;
    }

    /**
     * 获取record的分区的真实偏移量
     */
    public Map<TopicPartition, Long> getLastPartitionOffsetByTopic(String topic) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topic);
        Map<TopicPartition, Long> topicPartitionLongMap = super.kafkaConsumerService.endOffsets(topicPartitions);
        return topicPartitionLongMap;
    }

    /**
     * 获取record的最早的没有过期的偏移量
     */
    public Map<TopicPartition, Long> getEarliestPartitionOffsetByTopic(String topic) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topic);
        Map<TopicPartition, Long> topicPartitionLongMap = super.kafkaConsumerService.beginningOffsets(topicPartitions);
        return topicPartitionLongMap;
    }

    /**
     * 获取 topic 的指定时间戳之后的第一个 offset
     */
    public Map<TopicPartition, OffsetAndTimestamp> getFirstPartitionOffsetAfterTimestamp(String topic, Long timestamp) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topic);
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();

        topicPartitions.forEach(topicPartition -> {
            timestampsToSearch.put(topicPartition, timestamp);
        });

        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = super.kafkaConsumerService.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap;
    }
}