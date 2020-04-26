package demo.kafka.controller.consume.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Slf4j
public class KafkaConsumerService<K, V> implements Consumer<K, V> {



    private KafkaConsumerService() {
    }

    private KafkaConsumerService(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * 用于设置kafka的produce
     */
    private KafkaConsumer kafkaConsumer;


    /**
     * 列出所有的 Topic 的分区数据
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return kafkaConsumer.listTopics();
    }

    /**
     * 列出所有的 Topic 的分区数据
     *
     * @param timeout 等待时间
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return kafkaConsumer.listTopics(timeout);
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     * 获取被停止的 partition
     *
     * @return
     */
    @Override
    public Set<TopicPartition> paused() {
        return kafkaConsumer.paused();
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(Duration)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     *
     * @describe :
     * 暂停获取指定 partition的数据
     * 执行函数后
     * 1.poll函数不会获取指定 partition 的任何记录
     * 2.不会影响partition的订阅 -> 不会引起再平衡
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(Duration)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     *
     * @describe :
     * 恢复指定 partition 的订阅
     * 如果 partition 之前没有被暂停，不会做任何操作
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     * <p>
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @describe :
     * 根据指定的 timestamp 来寻找 offsets
     * (这个是阻塞调用)
     * 1.返回的是指定 partition 的大于 timestamp 的最早的 offset
     * 2.如果msg版本是0.10.0之前的,msg 没有 timestamp , 就会返回 null
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch);
    }

    /**
     * @param timestampsToSearch
     * @param timeout            等待获取偏移量的最长时间
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     * <p>
     *
     * @describe :
     * 获取指定 partition 的第一个偏移量(不会改变当前消费者的 指定 partition的位置)
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.beginningOffsets(partitions);
    }

    /**
     * @param timeout 等待获取偏移量的最长时间
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.beginningOffsets(partitions, timeout);
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @describe :
     * 获取指定 partition 的end偏移量(不会改变当前消费者的 指定 partition的位置)
     * 1.不同的事务级别返回但是不一致的
     * 2.分区没有被写入,返回的是0
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.endOffsets(partitions);
    }

    /**
     * @param timeout 等待获取偏移量的最长时间
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.endOffsets(partitions, timeout);
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @describe :
     * 关闭消费者
     * 1.等待默认30秒去处理
     * 2.如果自动提交开启,如果可能的话，就会提交当前的offset
     * 3.wakeup方法不能中断这个close
     */
    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        kafkaConsumer.close(timeout, unit);
    }

    /**
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     * @describe :
     * 等待的最大时间，优雅的关闭consumer
     * 1.必须非负
     * 2.指定为0,代表不等待
     */
    @Override
    public void close(Duration timeout) {
        kafkaConsumer.close(timeout);
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     *
     * @describe :
     * 唤醒 consumer
     * 1.是线程安全的
     * 2.作用于长时间的 poll
     */
    @Override
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     *
     * @return The set of partitions currently assigned to this consumer
     * @describe :
     * 获取当前消费者被分配的 partitions
     * 1.如果是通过 assign 函数订阅的，就会简单的返回同样的分区
     * 2.如果通过 subscription 订阅,就会返回被分配的 topic partition
     * 3.如果分配没有发生，或者是再分配状态 -> 返回 none
     */
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     *
     * @describe :
     * 获取当前订阅
     * 1.返回same最近调用 subscribe 指定的
     * 2.如果没有调用，返回就是 empty
     */
    @Override
    public Set<String> subscription() {
        return kafkaConsumer.subscription();
    }

    /**
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * <p>
     * 这个是no-listener的subscribe函数
     */
    @Override
    public void subscribe(Collection<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     * <p>
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * @describe :
     * 订阅给定的的 topics , 去获取动态的 partition
     * 1.订阅的topic不是增加而是替换
     * 2.使用手动分配的(assign),不会被管理
     * 3.如果给定的 topic 是empty, 就会像调用 unsubscribe 一样
     *
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>Number of partitions change for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * <li>An existing member of the consumer group is shutdown or fails
     * <li>A new member is added to the consumer group
     * </ul>
     * @describe :
     * 作为group管理的一部分
     * 1.consumer将会keep追踪属于这个group的partition
     * 2.将会触发rebalance操作,如果以下事件发生:
     * <p>
     * 2.1 订阅 topics 的 partition 数量的改变
     * 2.2 被订阅 topic 删除或者创建
     * 2.3 consumer 已经存在的成员停止或者失败
     * 2.4 consumer 的添加新成员
     *
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that rebalances will only occur during an active call to {@link #poll(Duration)}, so callbacks will
     * also only be invoked during that time.
     * @describe :
     * 当 rebalance 发生时，提供的监听器将会被第一次调用，
     * 1.表明consumer的分配已经被撤销 -> onPartitionsRevoked
     * 2.表明新的分配再一次被接收 -> onPartitionsAssigned
     * 注意:
     * rebalance 仅仅发生在调用 poll 的时候
     *
     *
     * <p>
     * The provided listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     * @describe :
     * 提供的listener将会立刻override之前的设置的 any listener
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(topics, callback);
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     * <p>
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered before the new
     * assignment replaces the old one.
     *
     * @describe :
     * 手动的分配 Topics 给 consumer
     * 1.这个不允许增加assignment
     * 2.将会替换之前的assignment
     * 如果给定的topic是empty -> 等价于 unsubscribe()
     * <p>
     * 这种方式分配的assignment，不会使用consumer的group管理(注意不能同时使用assign 和 subscribe)
     * -> 当group成员,集群,topic元数据发生改变,不会触发再均衡
     * -> 如果启用自动提交，替换 assignment 的时候会提交 async commit
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        kafkaConsumer.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        kafkaConsumer.subscribe(pattern);
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     *
     * @describe :
     * <p>
     * 1.解除订阅 subscribe 订阅的Topic
     * 2.解除订阅 assign 订阅的Topic
     */
    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }


    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return kafkaConsumer.poll(timeout);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
     * If the timeout expires, an empty record set will be returned. Note that this method may block beyond the
     * timeout in order to execute custom {@link ConsumerRebalanceListener} callbacks.
     *
     * @param timeout
     * @describe :
     * <p>
     * 从订阅/分配的Partition获取数据(如果没有订阅，调用poll会报错)
     * <p>
     * 每一的 poll(轮询), consumer会尝试使用最后的消费过的offset，作为开始的offset去依序的获取数据
     * 1.最新的消费过的offset可以使用 Seek 来设置
     * 2.最新的消费过的offset也可以自动的设置(自动提交)
     * <p>
     * 1.如果可以立刻获取 record , 就会立刻return
     * 2.如果不可以立刻获取 record , 将会等待超时
     * 3.如果超时就会返回 empty set 的 record
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return kafkaConsumer.poll(timeout);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     *
     * @describe :
     * 提交 poll 返回的最新的 offset(订阅的Partition)
     */
    @Override
    public void commitSync() {
        kafkaConsumer.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        kafkaConsumer.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        kafkaConsumer.commitSync(offsets);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @describe :
     * 提交指定的Partition 的指定 offsets
     * <p>
     * 这个提交 offset 到kafka
     * 1.这些被提交的offset将会被每次rebalane/启动
     * 2.如果希望存储 offset在非 kafka的其他位置,这个commitSync不建议调用
     * 3.commitSync是同步函数
     * 4.注意commitAsync的调用要先于这个函数的调用
     */

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        kafkaConsumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        kafkaConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(callback);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     * <p>
     *
     * @describe :
     * 前面大致和 commitSync 是一致的
     * 不同:
     * commitAsync 是同步非阻塞函数
     * 1.多次调用commitAsync会按照次序去commit
     * 2.对应从callback也会按照次序去调用
     * 3.注意确保在调用 commitSync 之前完成 commitAsync 的调用
     */
    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        kafkaConsumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        kafkaConsumer.seek(partition, offset);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets. This
     * method allows for setting the leaderEpoch along with the desired offset.
     *
     * @describe :
     * 覆盖获取的offset
     * 1.将会使用在下一次poll
     * 2.多次调用在同一个Partition上,最新的offset将会使用在next poll
     * 3.任意的使用这个函数可能丢失数据
     * 4.这个函数允许单独设定leader的偏移量???
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        kafkaConsumer.seek(partition, offsetAndMetadata);
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @describe :
     * seek 指定的 partitions 到offset的开头
     * 1.这个函数是懒执行,第一次的poll之后才会执行
     * 2.如果没有提供partition,seek当前分配的所有的partition的offset为第一个
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToBeginning(partitions);
    }

    /**
     * 参考 {@link #seekToBeginning(Collection)}}
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        kafkaConsumer.seekToEnd(partitions);
    }

    /**
     * 参考 {@link #position(TopicPartition, Duration)}
     */
    @Override
    public long position(TopicPartition partition) {
        return kafkaConsumer.position(partition);
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position
     * for the given partition.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @describe :
     * 获取下一个record的offset
     */
    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.position(partition, timeout);
    }


    /**
     * {@link #committed(TopicPartition, Duration)}
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return kafkaConsumer.committed(partition);
    }


    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will do a remote call to get the latest committed offset from the server, and will block until the
     * committed offset is gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @describe :
     * 获取指定 partition 的最新的已经提交的 offset
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.committed(partition, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> set, Duration duration) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @describe :
     * 获取给定的topic的元数据
     * (如果给定的topic没有元数据,将会触发远程调用)
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return kafkaConsumer.partitionsFor(topic, timeout);
    }

    public Map<String, List<Metric>> metricGroupNameMap() {
        Map<MetricName, ? extends Metric> metricNameMap = kafkaConsumer.metrics();
        Map<String, List<Metric>> metricGroupNameMap = new HashMap<>();
        metricNameMap.forEach((name, metric) -> {
            String groupName = metric.metricName().group();
            if (!metricGroupNameMap.containsKey(groupName)) {

                List<Metric> metrics = new ArrayList<>(Arrays.asList(metric));
                metricGroupNameMap.put(groupName, metrics);

            } else {
                metricGroupNameMap.get(groupName).add(metric);
            }

        });
        return metricGroupNameMap;
    }


}
