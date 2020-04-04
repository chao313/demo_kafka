package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminTopicUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminTopicTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        adminClient = AdminClient.create(properties);
    }


    /**
     * 测试获取全部的 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = AdminTopicUtil.listTopics(adminClient);
        listTopicsResult.names().get().forEach(name -> {
            log.info("name:{}", name);
        });
    }


    /**
     * 测试创建 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void createTopics() throws Exception {
        boolean bool = AdminTopicUtil.createTopic(adminClient, "Test11", 1, (short) 1);
        log.info("创建topic:{}", bool);

    }

    /**
     * 测试删除 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void deleteTopics() throws ExecutionException, InterruptedException {
        boolean bool = AdminTopicUtil.deleteTopics(adminClient, "Test11");
        log.info("删除topic:{}", bool);
    }

    /**
     * 测试删除 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void existTopicName() throws ExecutionException, InterruptedException {
        boolean bool = AdminTopicUtil.existTopicName(adminClient, "Test11");
        log.info("topic是否存在:{}", bool);
    }


    /**
     * 测试获取 topic 的细节
     */
    @Test
    public void describeTopic() throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = AdminTopicUtil.describeTopic(adminClient, "Test11");
        log.info("topic是否存在:{}", describeTopicsResult);
    }

}
