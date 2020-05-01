package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

@Data
public class ConsumerTopicAndPartitionsAndOffset {
    private String topic;//topic
    private int partition;
    private Long earliestOffset;//最早有效的 offset
    private Long lastOffset;//最新的 offset
    private Long sum;//目前有效的 offset的数量
    private String earliestTimestamp;//最早有效的 timestamp
    private String lastTimestamp;//最新 timestamp
    private ConsumerRecord earliestConsumerRecord;//最新的记录
    private ConsumerRecord lastConsumerRecord;//最新的记录
    private List<ConsumerRecord> lastTenConsumerRecords;//最新的记录
}
