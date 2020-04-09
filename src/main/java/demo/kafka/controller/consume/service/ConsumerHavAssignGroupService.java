package demo.kafka.controller.consume.service;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerHavAssignGroupService<K, V> extends ConsumerService<K, V> {

    ConsumerHavAssignGroupService(KafkaConsumerService kafkaConsumerService) {
        super(kafkaConsumerService);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerHavAssignGroupService<K, V> getInstance(KafkaConsumerService kafkaConsumerService, Collection<String> topics) {
        ConsumerHavAssignGroupService consumerHavAssignGroupService = new ConsumerHavAssignGroupService(kafkaConsumerService);
        Collection<TopicPartition> partitionInfos = new ArrayList<>();
        topics.forEach(topic -> {
            partitionInfos.addAll(getTopicPartitionsByTopic(consumerHavAssignGroupService, topic));
        });
        consumerHavAssignGroupService.getKafkaConsumerService().assign(topics);
        return consumerHavAssignGroupService;
    }


    /**
     * 根据 partition 来获取下一个偏移量
     */
    public long getNextOffsetByTopicAndPartition(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        super.kafkaConsumerService.assign(Arrays.asList(topicPartition));
        return super.kafkaConsumerService.position(topicPartition);
    }


    /**
     * 根据 topic 来生成 TopicPartitions
     */
    private static Collection<TopicPartition> getTopicPartitionsByTopic(ConsumerService consumerHavAssignGroupService, String topic) {
        Collection<PartitionInfo> partitionInfos = consumerHavAssignGroupService.getKafkaConsumerService().partitionsFor(topic);

        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitionInfos.forEach(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            topicPartitions.add(topicPartition);
        });
        return topicPartitions;
    }

}
