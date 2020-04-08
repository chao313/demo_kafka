package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.List;

@Data
public class TopicPartitionInfoResponse {
    private int partition;
    private Node leader;
    private List<Node> replicas;
    private List<Node> isr;

    public TopicPartitionInfoResponse(TopicPartitionInfo topicPartitionInfo) {
        this.partition = topicPartitionInfo.partition();
        this.leader = topicPartitionInfo.leader();
        this.replicas = topicPartitionInfo.replicas();
        this.isr = topicPartitionInfo.isr();
    }

    /**
     * 批量转换
     *
     * @param topicPartitionInfos
     * @return
     */
    public static List<TopicPartitionInfoResponse> addAll(List<TopicPartitionInfo> topicPartitionInfos) {
        List<TopicPartitionInfoResponse> topicPartitionInfoResponses = new ArrayList<>();
        topicPartitionInfos.forEach(topicPartitionInfo -> {
            topicPartitionInfoResponses.add(new TopicPartitionInfoResponse(topicPartitionInfo));
        });
        return topicPartitionInfoResponses;
    }
}
