package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminClusterService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminClusterTest {

    AdminClusterService adminClusterService = AdminClusterService.getInstance(Bootstrap.PROD_WIND.getIp());

    /**
     * 获取集群的信息(可以获取节点的信息)
     * <p>
     * node -> value=[10.200.126.163:9092 (id: 0 rack: null)],exception=null,done=true
     */
    @Test
    public void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = adminClusterService.getCluster();
        log.info("describeClusterResult:{}", describeClusterResult);
        log.info("describeClusterResult.nodes:{}", describeClusterResult.nodes());
        log.info("describeClusterResult.clusterId:{}", describeClusterResult.clusterId());
        log.info("describeClusterResult.controller:{}", describeClusterResult.controller());

    }

    /**
     * 测试 获取集群中的 brokers
     */
    @Test
    public void getBrokersInCluster() throws ExecutionException, InterruptedException {
        Collection<Node> nodes = adminClusterService.getBrokersInCluster();
        nodes.forEach(node -> {
            log.info("node:{}", node);
        });

    }


}
