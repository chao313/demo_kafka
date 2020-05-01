package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * 辅助集合全部的 partition的数据
 */
@Data
public class ConsumerTopicOffset {
    private String topic;//topic
    private int partitions;//partition的数量
    private Long sum;//目前有效的 offset的数量
    private Long total;//offset的总和
    private String earliestTimestamp;//最早msg的时间
    private String lastTimestamp;//最新 timestamp
}
