package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminClusterUtil;
import demo.kafka.controller.admin.util.AdminTopicUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminClusterTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        adminClient = AdminClient.create(properties);
    }


    /**
     * 获取集群的信息(可以获取节点的信息)
     * <p>
     * node -> value=[10.200.126.163:9092 (id: 0 rack: null)],exception=null,done=true
     */
    @Test
    public void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = AdminClusterUtil.describeCluster(adminClient);
        log.info("describeClusterResult:{}", describeClusterResult);
        log.info("describeClusterResult.nodes:{}", describeClusterResult.nodes());
        log.info("describeClusterResult.clusterId:{}", describeClusterResult.clusterId());
        log.info("describeClusterResult.controller:{}", describeClusterResult.controller());

    }


}
