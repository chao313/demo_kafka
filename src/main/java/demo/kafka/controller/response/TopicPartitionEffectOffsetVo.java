package demo.kafka.controller.response;

import lombok.Data;

/**
 * 记录 topicPartition 的有效的开始和结束的offset
 */
@Data
public class TopicPartitionEffectOffsetVo {
    private String topic;//topic
    private int partition;
    private Long earliestOffset;//最早有效的 offset
    private Long lastOffset;//最新的 offset
}
