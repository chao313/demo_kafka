package demo.kafka.controller.consume.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * <p>
 * 1.操作Partition的分配(重新分配和获取当前被分配的Partition)
 * 2.获取指定Partition的下一个要消费的offset
 * 3.操作消费的Partition的offset(设置offset到开始,结尾,任意)
 * 4.操作消费本身(暂停/恢复)
 */
@Slf4j
public class ConsumerHavGroupAssignService<K, V> extends ConsumerNoGroupService<K, V> {
    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     */
    protected static <K, V> ConsumerHavGroupAssignService<K, V> getInstance(KafkaConsumer<K, V> kafkaConsumer) {
        return new ConsumerHavGroupAssignService<K, V>(kafkaConsumer);
    }

    ConsumerHavGroupAssignService(KafkaConsumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
    }


    /**
     * 构造函数(直接注入 kafkaConsumer 和 需要 assign的topic)
     */
    public static <K, V> ConsumerHavGroupAssignService<K, V> getInstance(KafkaConsumer<K, V> kafkaConsumer, String topic) {
        return new ConsumerHavGroupAssignService<K, V>(kafkaConsumer, topic);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer 和 需要 assign的topic)
     */
    public static <K, V> ConsumerHavGroupAssignService<K, V> getInstance(KafkaConsumer<K, V> kafkaConsumer, TopicPartition topicPartition) {
        return new ConsumerHavGroupAssignService<K, V>(kafkaConsumer, topicPartition);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer 和 需要 assign的topic)
     */
    public static <K, V> ConsumerHavGroupAssignService<K, V> getInstance(KafkaConsumer<K, V> kafkaConsumer, String topic, int partition) {
        return new ConsumerHavGroupAssignService<K, V>(kafkaConsumer, topic, partition);
    }

    ConsumerHavGroupAssignService(KafkaConsumer<K, V> kafkaConsumer, String topic) {
        super(kafkaConsumer);
        Collection<TopicPartition> topicPartitionsByTopic = super.getTopicPartitionsByTopic(topic);
        super.getConsumer().assign(topicPartitionsByTopic);
    }

    ConsumerHavGroupAssignService(KafkaConsumer<K, V> kafkaConsumer, TopicPartition topicPartition) {
        super(kafkaConsumer);
        super.getConsumer().assign(Arrays.asList(topicPartition));
    }


    ConsumerHavGroupAssignService(KafkaConsumer<K, V> kafkaConsumer, String topic, int partition) {
        super(kafkaConsumer);
        super.getConsumer().assign(Arrays.asList(new TopicPartition(topic, partition)));
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
        log.info("尝试获取一批数据...:{}", records.count());
    }


    /**
     * 直接获取一批数据
     *
     * @return
     */
    public ConsumerRecords<K, V> pollOnce() {
        ConsumerRecords<K, V> records;
        records = this.getConsumer().poll(Duration.ofMillis(1000));
        return records;
    }


    /**
     * 查看分配到的Partition
     */
    public Set<TopicPartition> getPartitionAssigned() {
        return this.getConsumer().assignment();
    }

    /**
     * update新的Partition
     * -> 调用之后 {@link #getPartitionAssigned()}  就会改变
     */
    public Collection<TopicPartition> updatePartitionAssign(String topic) {
        Collection<TopicPartition> topicPartitionsToBeAssigned = super.getTopicPartitionsByTopic(topic);
        this.getConsumer().assign(topicPartitionsToBeAssigned);
        return topicPartitionsToBeAssigned;
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
     * 把分配到的 partition 全部更新到最开始的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionAssignedOffsetToBeginning() {
        Set<TopicPartition> partitionToBeSeekBegin = this.getPartitionAssigned();
        this.getConsumer().seekToBeginning(partitionToBeSeekBegin);
        return partitionToBeSeekBegin;
    }

    /**
     * 把分配到的 partition 全部更新到最新的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    public Collection<TopicPartition> updatePartitionAssignedOffsetToEnd() {
        Set<TopicPartition> partitionToBeSeekEnd = this.getPartitionAssigned();
        this.getConsumer().seekToEnd(partitionToBeSeekEnd);
        return partitionToBeSeekEnd;
    }
//
//    /**
//     * 把分配到的 partition 全部更新到最新的偏移量
//     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
//     */
//    public Collection<TopicPartition> getLastRecordByTopicPartition(TopicPartition topicPartition) {
//        this.getKafkaConsumerService().seekToEnd(Arrays.asList(topicPartition));
//        ConsumerRecords<K, V> consumerRecords = this.pollOnce();
//        if (consumerRecords.){
//            return null;
//        }else {
//            return consumerRecords
//        }
//        return consumerRecords;
//    }

    /**
     * 把分配到的 partition 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    public Collection<TopicPartition> updatePartitionAssignedOffset(long offset) {
        Set<TopicPartition> partitionToBeSeek = this.getPartitionAssigned();
        partitionToBeSeek.forEach(partition -> {
            this.getConsumer().seek(partition, offset);
        });
        return partitionToBeSeek;
    }


    /**
     * 把 分配 到的 partition 全部 暂停
     * {@link #pollOnce(Consumer)} ()} 就会无法获取到值
     */
    public Collection<TopicPartition> updatePartitionAssignedToBePause() {
        Set<TopicPartition> partitionToBePause = this.getPartitionAssigned();
        this.getConsumer().pause(partitionToBePause);
        return partitionToBePause;
    }

    /**
     * 把 分配 到的 partition 全部 恢复
     * {@link #pollOnce(Consumer)} ()}就会正常获取到值
     */
    public Collection<TopicPartition> updatePartitionAssignedToBeResume() {
        Set<TopicPartition> partitionToBeResume = this.getPartitionAssigned();
        this.getConsumer().resume(partitionToBeResume);
        return partitionToBeResume;
    }

    /**
     * 获取record的最早的没有过期的Record (根据 TopicPartition)
     */
    public ConsumerRecord<K, V> getEarliestRecord(TopicPartition topicPartition) {
        super.consumer.poll(1000);
        super.consumer.seekToBeginning(Arrays.asList(topicPartition));
        ConsumerRecords<K, V> records = super.consumer.poll(1000);
        if (records.records(topicPartition).size() > 0) {
            return records.records(topicPartition).get(0);
        }
        return null;
    }

    /**
     * 获取record的最后的的没有过期的偏移量(根据 TopicPartition)
     */
    public ConsumerRecord<K, V> getLatestRecord(TopicPartition topicPartition) {
        Long earliestPartitionOffset = this.getEarliestPartitionOffset(topicPartition);
        super.consumer.poll(1000);
        Long lastPartitionOffset = this.getLastPartitionOffset(topicPartition);
        Long offset = earliestPartitionOffset;//默认为最新
        if (lastPartitionOffset > earliestPartitionOffset) {
            offset = lastPartitionOffset - 1;//移动到前一个
        }
        super.consumer.seek(topicPartition, offset);//调整到最后
        ConsumerRecords<K, V> records = super.consumer.poll(1000);
        if (records.records(topicPartition).size() > 0) {
            return records.records(topicPartition).get(0);
        }
        return null;
    }

    /**
     * 根据 Offset Record
     */
    public ConsumerRecord<K, V> getRecordByOffset(TopicPartition topicPartition, Long offset) {
        Long earliestPartitionOffset = this.getEarliestPartitionOffset(topicPartition);
        Long lastPartitionOffset = this.getLastPartitionOffset(topicPartition);

        if (offset < earliestPartitionOffset) {
            throw new RuntimeException("偏移量小于最小值:" + earliestPartitionOffset);
        }

        if (offset > lastPartitionOffset) {
            throw new RuntimeException("偏移量大于最大值" + lastPartitionOffset);
        }

        super.consumer.seek(topicPartition, offset - 1);//调整到最后
        ConsumerRecords<K, V> records = super.consumer.poll(0);
        if (records.records(topicPartition).size() > 0) {
            return records.records(topicPartition).get(0);
        }
        return null;
    }

}
