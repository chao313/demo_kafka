package demo.kafka.controller.admin.vo;

import lombok.Data;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;

import java.util.List;
import java.util.Set;

@Data
public class TopicDescriptionResponse {
    private String name;
    private boolean internal;
    private List<TopicPartitionInfoResponse> topicPartitionInfoResponses;//自己包裹的response
    private Set<AclOperation> authorizedOperations;

    private TopicDescription topicDescription;

    public TopicDescriptionResponse(TopicDescription topicDescription) {
        this.topicDescription = topicDescription;
        this.name = topicDescription.name();
        this.internal = topicDescription.isInternal();
        this.topicPartitionInfoResponses = TopicPartitionInfoResponse.addAll(topicDescription.partitions());
        this.authorizedOperations = topicDescription.authorizedOperations();
    }
}
