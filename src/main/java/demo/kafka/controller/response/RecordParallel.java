package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

@Data
public class RecordParallel {
    public String bootstrap_servers;
    public TopicPartition topicPartition;
    public long startOffset;
    public long endOffset;
    public String keyRegex;
    public String valueRegex;
    public Long timeStartOriginal;
    public Long timeEndOriginal;

    public RecordParallel(String bootstrap_servers, TopicPartition topicPartition, long startOffset, long endOffset, String keyRegex, String valueRegex, Long timeStartOriginal, Long timeEndOriginal) {
        this.bootstrap_servers = bootstrap_servers;
        this.topicPartition = topicPartition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.keyRegex = keyRegex;
        this.valueRegex = valueRegex;
        this.timeStartOriginal = timeStartOriginal;
        this.timeEndOriginal = timeEndOriginal;
    }

    public static List<RecordParallel> generate(String bootstrap_servers,
                                                TopicPartition topicPartition,
                                                long startOffset,
                                                long endOffset,
                                                String keyRegex,
                                                String valueRegex,
                                                Long timeStartOriginal,
                                                Long timeEndOriginal,
                                                int split

    ) {
        List<RecordParallel> result = new ArrayList<>();
        for (long i = startOffset; i <= endOffset; ) {
            long tmp = i;
            i = i + split;
            RecordParallel recordParallel = new RecordParallel(bootstrap_servers, topicPartition, tmp, i, keyRegex, valueRegex, timeStartOriginal, timeEndOriginal);
            result.add(recordParallel);
        }
        return result;

    }

}
