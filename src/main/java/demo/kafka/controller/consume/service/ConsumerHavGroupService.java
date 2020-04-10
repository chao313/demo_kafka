package demo.kafka.controller.consume.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 抽取的 havGroup 应该有的函数
 *
 * @param <K>
 * @param <V>
 */

public interface ConsumerHavGroupService<K, V> {


    void pollOnce(Consumer<ConsumerRecord<K, V>> consumer);

    long getNextOffsetByTopicAndPartition(String topic, int partition);

    Set<TopicPartition> getPartitionAssigned();

    Collection<TopicPartition> updatePartitionAssign(String topic);

    Collection<TopicPartition> updatePartitionAssignedOffsetToBeginning();

    Collection<TopicPartition> updatePartitionAssignedOffsetToEnd();

    Collection<TopicPartition> updatePartitionAssignedOffset(long offset);

    Collection<TopicPartition> updatePartitionAssignedToBePause();

    Collection<TopicPartition> updatePartitionAssignedToBeResume();
}
