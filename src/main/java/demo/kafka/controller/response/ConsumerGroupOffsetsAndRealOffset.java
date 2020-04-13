package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

@Data
public class ConsumerGroupOffsetsAndRealOffset {
    private OffsetAndMetadata offsetAndMetadata;
    private Long startOffset;
    private Long endOffset;
    private String topic;
    private int partition;

}
