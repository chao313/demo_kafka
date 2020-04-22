package demo.kafka.controller.response;

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

/**
 * 用于解析 offset 提交的数据
 */
@Data
public class OffsetRecordResponse {

    public static final String TypeGroupMetadataKey = "GroupMetadataKey";
    public static final String TypeBaseKey = "BaseKey";
    public static final String TypeNULL = "NULL";

    private String type;//自定义的属性，记录当前的offset的类型是 GroupMetadataKey | BaseKey

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

    public OffsetRecordResponse(ConsumerRecord<byte[], byte[]> consumerRecord) {
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        this.timestamp = consumerRecord.timestamp();
        this.timestampType = consumerRecord.timestampType();
        this.serializedKeySize = consumerRecord.serializedKeySize();
        this.serializedValueSize = consumerRecord.serializedValueSize();
        this.headers = consumerRecord.headers();
        this.baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(consumerRecord.key()));
        this.key = this.baseKey.toString();
        this.leaderEpoch = consumerRecord.leaderEpoch();
        if (null != consumerRecord.value()) {
            if (this.baseKey instanceof GroupMetadataKey) {
                /**key 的格式为 GroupMetadataKey -> value是各个消费者信息 */
                this.value = GroupMetadataManager.readGroupMessageValue(((GroupMetadataKey) this.baseKey).key(), ByteBuffer.wrap(consumerRecord.value()), Time.SYSTEM);
                this.type = TypeGroupMetadataKey;
            } else {
                /**key 的格式为 BaseKey -> value对应的消费者各个Partition的offset */
                this.value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(consumerRecord.value()));
                this.type = TypeBaseKey;
            }
        } else {
            //这个是Value为null的情况
            this.type = TypeNULL;
        }
    }

    /**
     * 批量处理
     *
     * @param consumerRecords
     * @return
     */
    public static List<OffsetRecordResponse> getList(List<ConsumerRecord<byte[], byte[]>> consumerRecords) {
        List<OffsetRecordResponse> result = new ArrayList<>();
        consumerRecords.forEach(consumerRecord -> {
            result.add(new OffsetRecordResponse(consumerRecord));
        });
        return result;
    }
}
