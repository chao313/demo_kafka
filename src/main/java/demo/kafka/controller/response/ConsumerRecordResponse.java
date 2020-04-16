package demo.kafka.controller.response;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataKey;
import kafka.coordinator.group.GroupMetadataManager;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
public class ConsumerRecordResponse {

    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private TimestampType timestampType;
    private int serializedKeySize;
    private int serializedValueSize;
    private Headers headers;
    private BaseKey baseKey;
    private String key;
    private Object value;
    private Optional<Integer> leaderEpoch;

    public ConsumerRecordResponse(ConsumerRecord<byte[], byte[]> consumerRecord) {
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        this.timestampType = consumerRecord.timestampType();
        this.serializedKeySize = consumerRecord.serializedKeySize();
        this.serializedValueSize = consumerRecord.serializedValueSize();
        this.headers = consumerRecord.headers();
        this.baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(consumerRecord.key()));
        this.key = this.baseKey.toString();
        if (null != consumerRecord.value()) {
            if (this.baseKey instanceof GroupMetadataKey) {
                /**key 的格式为 GroupMetadataKey -> value是各个消费者信息 */
                this.value = GroupMetadataManager.readGroupMessageValue(((GroupMetadataKey) this.baseKey).key(), ByteBuffer.wrap(consumerRecord.value()), Time.SYSTEM);
            } else {
                /**key 的格式为 BaseKey -> value对应的消费者各个Partition的offset */
                this.value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(consumerRecord.value()));
            }
        }
        this.leaderEpoch = consumerRecord.leaderEpoch();
    }

    /**
     * 批量处理
     *
     * @param consumerRecords
     * @return
     */
    public static List<ConsumerRecordResponse> getList(List<ConsumerRecord<byte[], byte[]>> consumerRecords) {
        List<ConsumerRecordResponse> result = new ArrayList<>();
        consumerRecords.forEach(consumerRecord -> {
            result.add(new ConsumerRecordResponse(consumerRecord));
        });
        return result;
    }
}
