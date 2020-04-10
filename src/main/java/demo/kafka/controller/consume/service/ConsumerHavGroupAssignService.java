package demo.kafka.controller.consume.service;

import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;

public class ConsumerHavGroupAssignService<K, V> extends ConsumerNoGroupService<K, V> {

    ConsumerHavGroupAssignService(KafkaConsumerService kafkaConsumerService, String topic) {
        super(kafkaConsumerService);
        Collection<TopicPartition> topicPartitionsByTopic = super.getTopicPartitionsByTopic(topic);
        super.getKafkaConsumerService().assign(topicPartitionsByTopic);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer 和 需要 assign的topic)
     */
    public static <K, V> ConsumerHavGroupAssignService<K, V> getInstance(KafkaConsumerService kafkaConsumerService, String topic) {
        return new ConsumerHavGroupAssignService<>(kafkaConsumerService, topic);
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
