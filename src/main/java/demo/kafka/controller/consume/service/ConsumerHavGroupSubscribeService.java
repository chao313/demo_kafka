package demo.kafka.controller.consume.service;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ConsumerHavGroupSubscribeService<K, V> extends ConsumerService<K, V> {

    ConsumerHavGroupSubscribeService(KafkaConsumerService kafkaConsumerService) {
        super(kafkaConsumerService);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerHavGroupSubscribeService<K, V> getInstance(KafkaConsumerService kafkaConsumerService, Collection<String> topics) {
        ConsumerHavGroupSubscribeService consumerHavAssignGroupService = new ConsumerHavGroupSubscribeService(kafkaConsumerService);
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
