package demo.kafka.controller.consume.service;

import demo.kafka.controller.response.OffsetRecordResponse;
import demo.kafka.util.MapUtil;
import kafka.coordinator.group.GroupMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class KafkaOffsetService {

    public static final String _consumer_offsets = "__consumer_offsets";
    public static Integer _consumer_offsets_size = 50;//默认的分区的size

    /**
     * 设置 分区 size
     */
    public static void set_consumer_offsets_size(Integer _consumer_offsets_size) {
        KafkaOffsetService._consumer_offsets_size = _consumer_offsets_size;
    }


    /**
     * 获取指定group的最新的groupMeta的offset记录
     */
    public static OffsetRecordResponse getLastGroupMetadataOffsetRecord(
            String bootstrap_servers,
            String groupId) {

        KafkaConsumerService<byte[], byte[]> instance
                = KafkaConsumerService.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"
                ));

        /**算出group所属分区*/
        int partition = Math.abs(groupId.hashCode() % _consumer_offsets_size);//确地group对应的partition
        /**
         * 生成kafka 的 topicPartition
         */
        TopicPartition topicPartition = new TopicPartition(_consumer_offsets, partition);

        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService
                = ConsumerHavGroupAssignService.getInstance(instance, _consumer_offsets, partition);


        Long earliestPartitionOffset = consumerHavGroupAssignService.getEarliestPartitionOffset(topicPartition);
        Long lastPartitionOffset = consumerHavGroupAssignService.getLastPartitionOffset(topicPartition);

        instance.assign(Arrays.asList(new TopicPartition(_consumer_offsets, partition)));

        OffsetRecordResponse offsetRecordResponse = null;

        /**
         * 从最后一个开始，向前遍历
         */
        if (lastPartitionOffset > earliestPartitionOffset) {
            for (long offset = lastPartitionOffset; offset > earliestPartitionOffset; offset--) {
                instance.seek(topicPartition, offset);
                ConsumerRecords<byte[], byte[]> records = instance.poll(Duration.ofMillis(10));
                List<OffsetRecordResponse> offsetRecordRespons = OffsetRecordResponse.getList(records.records(topicPartition));
                for (OffsetRecordResponse recordResponse : offsetRecordRespons) {
                    if (recordResponse.getKey().contains(groupId)) {
                        //是指定group的offset
                        log.info("recordResponse:{}", recordResponse);
                        log.info("recordResponse.getType:{}", recordResponse.getType());
                        if (recordResponse.getType().equalsIgnoreCase(OffsetRecordResponse.TypeGroupMetadataKey)) {
                            //如果是 GroupMetadataKey 类型的提交
                            log.info("当前group的最新的GroupMetadataKey提交:{}", recordResponse);
                            offsetRecordResponse = recordResponse;
                            instance.close();
                            return offsetRecordResponse;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 获取指定group订阅的topic
     */
    public static Set<String> getSubscribedTopics(String bootstrap_servers,
                                                  String groupId) {
        Set<String> topics = new HashSet<>();
        OffsetRecordResponse lastGroupMetadataOffsetRecord
                = KafkaOffsetService.getLastGroupMetadataOffsetRecord(bootstrap_servers, groupId);
        if (null != lastGroupMetadataOffsetRecord) {
            if ((lastGroupMetadataOffsetRecord.getValue() instanceof GroupMetadata)) {
                //如果是 GroupMetadata 类型
                ((GroupMetadata) lastGroupMetadataOffsetRecord.getValue()).allMemberMetadata().forall(memberMetadata -> {
                    String topic = new String(memberMetadata.assignment());
                    topics.add(topic);
                    return true;
                });
            }
        }
        return topics;
    }

}
