package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminClusterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminPartitionsUtilTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "10.200.126.163:9092");
        adminClient = AdminClient.create(properties);
    }


    /**
     * 获取集群的信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = AdminClusterUtil.describeCluster(adminClient);
        log.info("describeClusterResult:{}", describeClusterResult);
    }


}
