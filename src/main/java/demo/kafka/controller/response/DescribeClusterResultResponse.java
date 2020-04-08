package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Data
public class DescribeClusterResultResponse {
    private Collection<Node> nodes;
    private Node controller;
    private String clusterId;
    private Set<AclOperation> authorizedOperations;

    public DescribeClusterResultResponse(Collection<Node> nodes, Node controller, String clusterId, Set<AclOperation> authorizedOperations) {
        this.nodes = nodes;
        this.controller = controller;
        this.clusterId = clusterId;
        this.authorizedOperations = authorizedOperations;
    }

    /**
     * 批量转换
     */
    public static DescribeClusterResultResponse addAll(DescribeClusterResult describeClusterResult)
            throws ExecutionException, InterruptedException {
        return new DescribeClusterResultResponse(
                describeClusterResult.nodes().get(),
                describeClusterResult.controller().get(),
                describeClusterResult.clusterId().get(),
                describeClusterResult.authorizedOperations().get());
    }

}
