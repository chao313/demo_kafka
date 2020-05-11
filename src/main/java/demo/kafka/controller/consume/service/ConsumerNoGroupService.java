package demo.kafka.controller.consume.service;

import demo.kafka.controller.consume.service.base.ConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 1.读取全部的topic
 * 2.读取全部的partition
 * 3.获取全部partition的offset的最新和最旧的offset及(根据时间戳获取offset)
 *
 * @param <K>
 * @param <V>
 */

@Slf4j
public class ConsumerNoGroupService<K, V> extends ConsumerService<K, V> {

    ConsumerNoGroupService(KafkaConsumer<K, V> kafkaConsumer) {
        super(kafkaConsumer);
    }

    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     */
    protected static <K, V> ConsumerNoGroupService<K, V> getInstance(KafkaConsumer<K, V> kafkaConsumer) {
        return new ConsumerNoGroupService(kafkaConsumer);
    }

    /**
     * 根据 获取全部的 Topic 和Partitions
     */
    public Map<String, List<PartitionInfo>> getAllTopicAndPartitions() {
        return super.consumer.listTopics();

    }

    /**
     * 获取全部的 topic
     */
    public Set<String> getAllTopics() {
        return super.consumer.listTopics().keySet();

    }

    /**
     * 根据 topic 获取 partitions
     */
    public Collection<PartitionInfo> getPartitionsByTopic(String topic) {
        List<PartitionInfo> partitionInfos = super.consumer.partitionsFor(topic);
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
     * 获取全部的 TopicPartition (多个topic)
     */
    public Collection<TopicPartition> getTopicPartitionsByTopic(Collection<String> topics) {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topics.forEach(topic -> {
            topicPartitions.addAll(this.getTopicPartitionsByTopic(topic));
        });
        return topicPartitions;
    }

    /**
     * 根据获取全部的TopicPartitions
     */
    public Collection<TopicPartition> getAllTopicPartitions() {
        Set<String> allTopics = this.getAllTopics();
        return this.getTopicPartitionsByTopic(allTopics);
    }


    /**
     * 获取record的分区的真实偏移量
     */
    public Map<TopicPartition, Long> getLastPartitionOffset(String topic) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topic);
        Map<TopicPartition, Long> topicPartitionLongMap = super.consumer.endOffsets(topicPartitions);
        return topicPartitionLongMap;
    }


    /**
     * 获取record的分区的真实偏移量(多个topic)
     */
    public Map<TopicPartition, Long> getLastPartitionOffset(Collection<String> topics) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topics);
        Map<TopicPartition, Long> topicPartitionLongMap = super.consumer.endOffsets(topicPartitions);
        return topicPartitionLongMap;
    }

    /**
     * 获取record的最早的没有过期的偏移量(根据 TopicPartition)
     */
    public Long getLastPartitionOffset(TopicPartition topicPartition) {
        Map<TopicPartition, Long> topicPartitionLongMap =
                super.consumer.endOffsets(Arrays.asList(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    /**
     * 获取record的最早的没有过期的偏移量(根据 Topic)
     */
    public Map<TopicPartition, Long> getEarliestPartitionOffset(String topic) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topic);
        Map<TopicPartition, Long> topicPartitionLongMap = super.consumer.beginningOffsets(topicPartitions);
        return topicPartitionLongMap;
    }

    /**
     * 获取record的最早的没有过期的偏移量(根据 TopicPartition)
     */
    public Long getEarliestPartitionOffset(TopicPartition topicPartition) {
        Map<TopicPartition, Long> topicPartitionLongMap =
                super.consumer.beginningOffsets(Arrays.asList(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    /**
     * 获取record的最早的没有过期的偏移量(多个topic)
     */
    public Map<TopicPartition, Long> getEarliestPartitionOffset(Collection<String> topics) {
        Collection<TopicPartition> topicPartitions = this.getTopicPartitionsByTopic(topics);
        Map<TopicPartition, Long> topicPartitionLongMap = super.consumer.beginningOffsets(topicPartitions);
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
                = super.consumer.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap;
    }

    /**
     * 获取 topicPartition 的指定时间戳之后的第一个 OffsetAndTimestamp
     */
    public OffsetAndTimestamp getFirstPartitionOffsetAfterTimestamp(TopicPartition topicPartition, Long timestamp) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(topicPartition, timestamp);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = super.consumer.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap.get(topicPartition);
    }

    /**
     * 获取 topicPartition 的指定时间第一个 OffsetAndTimestamp(直接查询>0即可)
     */
    public OffsetAndTimestamp getFirstPartitionOffsetAndTimestamp(TopicPartition topicPartition) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(topicPartition, 0L);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = super.consumer.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap.get(topicPartition);
    }

    /**
     * 获取 topicPartition 的指定时间最后的(用二分法) OffsetAndTimestamp
     */
    public OffsetAndTimestamp getLastPartitionOffsetAndTimestamp(TopicPartition topicPartition) {
        Long lastPartitionOffset = this.getLastPartitionOffset(topicPartition);
        OffsetAndTimestamp offsetAndTimestamp = this.getOffsetAndTimestampByOffset(topicPartition, lastPartitionOffset - 1);
        return offsetAndTimestamp;

    }

    /**
     * 根据 offset 查询 OffsetAndTimestamp(二分)
     * ！！！ 这里会主动减1 -> endOffsets 返回的是下一个的偏移量
     * <p>
     * 这个会阻塞调用 {@link KafkaConsumerCommonService#getOffsetAndTimestampByOffset(String, TopicPartition, Long)} ()}
     * !!! 注意！：kafka无法精确到毫秒以下的
     * @param topicPartition
     * @param partitionOffset
     * @return
     */
    public OffsetAndTimestamp getOffsetAndTimestampByOffset(TopicPartition topicPartition, Long partitionOffset) {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        OffsetAndTimestamp firstPartitionOffsetAndTimestamp = this.getFirstPartitionOffsetAndTimestamp(topicPartition);
        if (null == firstPartitionOffsetAndTimestamp) {
            /**如果第一个就为null,代表没有最后*/
            return null;
        }
        /**获取最后一个offset*/
        Long endTime = new Date().getTime();//最新的时间
        Long startTime = firstPartitionOffsetAndTimestamp.timestamp();
        OffsetAndTimestamp offsetAndTimestamp = null;
        do {
            Long middleTime = (endTime + startTime) / 2;
//            log.info("二分法-startTime:{}-middle:{}-endTime:{}:offsetAndTimestamp:{}", startTime, middleTime, endTime, offsetAndTimestamp);
            offsetAndTimestamp = this.getFirstPartitionOffsetAfterTimestamp(topicPartition, middleTime);
            log.info("二分法-startTime:{}-middle:{}-endTime:{}:offsetAndTimestamp:{}", fastDateFormat.format(startTime), fastDateFormat.format(middleTime), fastDateFormat.format(endTime), offsetAndTimestamp);
            if (null == offsetAndTimestamp) {
                /**如过offsetAndTimestamp 为null -> 在右分 */
                endTime = middleTime + 1;//左移

            } else if (offsetAndTimestamp.offset() < partitionOffset) {
                /**如过offsetAndTimestamp < lastPartitionOffset  -> 在左分 */
                if (startTime == middleTime - 1) {
                    /**当start和 middleTime 相差1的时候,并且此时偏移量仍然小于目标offset -> 可能1毫秒就有几百条！！！*/
                    break;
                }
                startTime = middleTime - 1;//右移
            } else if (offsetAndTimestamp.offset() > partitionOffset) {
                endTime = middleTime + 1;//左移

            } else if (offsetAndTimestamp.offset() == partitionOffset) {
                /**如果 == 就退出 */
                break;
            }
        } while (true);

        return offsetAndTimestamp;
    }

    /**
     * 获取 topicPartition 的指定时间戳之后的第一个 offset
     */
    public Long getFirstOffsetAfterTimestamp(TopicPartition topicPartition, Long timestamp) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(topicPartition, timestamp);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = super.consumer.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap.get(topicPartition).offset();
    }

    /**
     * 获取 topicPartition 的指定时间戳之后的第一个 offset (集合操作-加速性能)
     */
    public Map<TopicPartition, OffsetAndTimestamp> getFirstOffsetAfterTimestamp(Map<TopicPartition, Long> timestampsToSearch) {
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = super.consumer.offsetsForTimes(timestampsToSearch);
        return topicPartitionOffsetAndTimestampMap;
    }

}
