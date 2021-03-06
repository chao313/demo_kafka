package demo.kafka.controller.consume.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
public class ConsumerHavGroupSubscribeService<K, V> extends ConsumerNoGroupService<K, V> {

    ConsumerHavGroupSubscribeService(KafkaConsumer<K,V> kafkaConsumer, Collection<String> topics) {
        super(kafkaConsumer);
        super.getConsumer().subscribe(topics);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerHavGroupSubscribeService<K, V> getInstance(KafkaConsumer<K,V> kafkaConsumer, Collection<String> topics) {
        ConsumerHavGroupSubscribeService consumerHavAssignGroupService = new ConsumerHavGroupSubscribeService(kafkaConsumer, topics);
        return consumerHavAssignGroupService;
    }


    /**
     * 查看订阅到的 topic
     */
    public Set<String> getTopicSubscribed() {
        return this.getConsumer().subscription();
    }


    /**
     * update 订阅的主题
     */
    public void updateTopicSubscribed(Collection<String> topics) {
        this.getConsumer().subscribe(topics);
    }

    /**
     * 普通的监听函数(只一次)
     */
    public void pollOnce(Consumer<ConsumerRecord<K, V>> consumer) {
        ConsumerRecords<K, V> records;
        records = this.getConsumer().poll(Duration.ofMillis(1000));
        records.forEach(record -> {
            consumer.accept(record);
        });
        this.getConsumer().commitSync();
        log.info("尝试获取一批数据...:{}", records.count());
    }


    /**
     * 根据 partition 来获取下一个偏移量
     * <p>
     * !!!! 这里会检查是否是assign的分配的分区！ 不是就会抛出异常 （必须poll）
     */
    public long getNextOffsetByTopicAndPartition(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        return super.consumer.position(topicPartition);
    }

    /**
     * 把订阅到的 topic 全部更新到最开始的偏移量
     * !!! No current assignment for partition Test-0
     * 注意：如果没有poll,就会报没有 assignment 异常
     */
    public Collection<TopicPartition> updateTopicSubscribedOffsetToBeginning() {
        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(this.getTopicSubscribed());
        this.getConsumer().seekToBeginning(allTopicSubscribedPartitions);
        return allTopicSubscribedPartitions;
    }

    /**
     * 把订阅到的 topic 全部更新到最新的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionSubscribedOffsetToEnd() {
        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(this.getTopicSubscribed());
        this.getConsumer().seekToEnd(allTopicSubscribedPartitions);
        return allTopicSubscribedPartitions;
    }

    /**
     * 把 订阅 到的 topic 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    public Collection<TopicPartition> updatePartitionSubscribedOffset(long offset) {
        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(this.getTopicSubscribed());
        allTopicSubscribedPartitions.forEach(partition -> {
            this.getConsumer().seek(partition, offset);
        });
        return allTopicSubscribedPartitions;
    }

    /**
     * 把 订阅 到的 topic （指定）更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    public Collection<TopicPartition> updatePartitionSubscribedOffset(Collection<String> topics, long offset) {

        if (!this.getTopicSubscribed().containsAll(topics)) {
            throw new RuntimeException("分配的topic不包含指定的topic,无法设置offset");
        }

        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(topics);
        allTopicSubscribedPartitions.forEach(partition -> {
            this.getConsumer().seek(partition, offset);
        });
        return allTopicSubscribedPartitions;
    }

    /**
     * 把 指定的到的 topic （指定）更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionSubscribedOffset(TopicPartition topicPartition, long offset) {
        if (!this.getTopicSubscribed().contains(topicPartition.topic())) {
            throw new RuntimeException("分配的topic不包含指定的topic,无法设置offset");
        }
        this.getConsumer().seek(topicPartition, offset);
        return Arrays.asList(topicPartition);
    }

    /**
     * 把订阅到的 topic 全部 暂停
     * {@link #pollOnce(Consumer)} ()} 就会无法获取到值
     */
    public Collection<TopicPartition> updatePartitionSubscribedToBePause() {
        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(this.getTopicSubscribed());
        this.getConsumer().pause(allTopicSubscribedPartitions);
        return allTopicSubscribedPartitions;
    }

    /**
     * 把订阅到的 topic 全部 恢复
     * {@link #pollOnce(Consumer)} ()}就会正常获取到值
     */
    public Collection<TopicPartition> updatePartitionSubscribedToBeResume() {
        Collection<TopicPartition> allTopicSubscribedPartitions = this.getTopicPartitionsByTopic(this.getTopicSubscribed());
        this.getConsumer().resume(allTopicSubscribedPartitions);
        return allTopicSubscribedPartitions;
    }

}
