package demo.kafka.controller.consume.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 没有偏移量的说法 -> 无法调用commit相关的的函数
 * -> 没有存储offset发地方
 * -> 特别容易丢offset!
 * <p>
 * 1.assign的初衷应该是 Partition(订阅的入口就是Partition )
 */
@Slf4j
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
     * 普通的监听函数(只一次)
     */
    public void pollOnce(Consumer<ConsumerRecord<K, V>> consumer) {
        ConsumerRecords<K, V> records;
        records = this.getKafkaConsumerService().poll(Duration.ofMillis(1000));
        records.forEach(record -> {
            consumer.accept(record);
        });
        log.info("尝试获取一批数据...:{}", records.count());
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
     * 查看分配到的Partition
     */
    public Set<TopicPartition> getPartitionAssigned() {
        return this.getKafkaConsumerService().assignment();
    }

    /**
     * update新的Partition
     *  -> 调用之后 {@link #getPartitionAssigned()}  就会改变
     */
    public Collection<TopicPartition> updatePartitionAssign(String topic) {
        Collection<TopicPartition> topicPartitionsToBeAssigned = super.getTopicPartitionsByTopic(topic);
        this.getKafkaConsumerService().assign(topicPartitionsToBeAssigned);
        return topicPartitionsToBeAssigned;
    }

    /**
     * 把订阅到的 partition 全部更新到最开始的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionAssignedOffsetToBeginning() {
        Set<TopicPartition> partitionToBeSeekBegin = this.getPartitionAssigned();
        this.getKafkaConsumerService().seekToBeginning(partitionToBeSeekBegin);
        return partitionToBeSeekBegin;
    }

    /**
     * 把订阅到的 partition 全部更新到最新的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionAssignedOffsetToEnd() {
        Set<TopicPartition> partitionToBeSeekEnd = this.getPartitionAssigned();
        this.getKafkaConsumerService().seekToEnd(partitionToBeSeekEnd);
        return partitionToBeSeekEnd;
    }

    /**
     * 把订阅到的 partition 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    public Collection<TopicPartition> updatePartitionAssignedOffset(long offset) {
        Set<TopicPartition> partitionToBeSeek = this.getPartitionAssigned();
        partitionToBeSeek.forEach(partition -> {
            this.getKafkaConsumerService().seek(partition, offset);
        });
        return partitionToBeSeek;
    }


    /**
     * 把订阅到的 partition 全部 暂停
     * {@link #pollOnce(Consumer)} ()} 就会无法获取到值
     */
    public Collection<TopicPartition> updatePartitionAssignedToBePause() {
        Set<TopicPartition> partitionToBePause = this.getPartitionAssigned();
        this.getKafkaConsumerService().pause(partitionToBePause);
        return partitionToBePause;
    }

    /**
     * 把订阅到的 partition 全部 恢复
     * {@link #pollOnce(Consumer)} ()}就会正常获取到值
     */
    public Collection<TopicPartition> updatePartitionAssignedToBeResume() {
        Set<TopicPartition> partitionToBeResume = this.getPartitionAssigned();
        this.getKafkaConsumerService().resume(partitionToBeResume);
        return partitionToBeResume;
    }

}