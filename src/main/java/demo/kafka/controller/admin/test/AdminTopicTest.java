package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.service.AdminTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminTopicTest {


    AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(Bootstrap.MY.getIp());


    /**
     * 测试获取全部的 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void getTopicsResults() throws ExecutionException, InterruptedException {
        Collection<TopicListing> topicListings = adminTopicService.getTopicsResults();
        topicListings.forEach(topicListing -> {
            log.info("topicListing:{}", topicListing);
        });
    }

    /**
     * 测试获取全部的 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void listTopicNames() throws ExecutionException, InterruptedException {
        Set<String> topicNames = adminTopicService.getTopicNames();
        topicNames.forEach(name -> {
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
        boolean bool = adminTopicService.addTopic("Test11", 1, (short) 1);
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
        boolean bool = adminTopicService.deleteTopic("Test11");
        log.info("删除topic:{}", bool);
    }

    /**
     * 测试删除 All topics
     */
    @Test
    public void deleteAllTopics() throws ExecutionException, InterruptedException {
        Set<String> topicNames = adminTopicService.getTopicNames();
        for (String topic : topicNames) {
            if (!topic.startsWith("_")) {
                /**
                 * 不删除 系统的 topic
                 */
                boolean bool = adminTopicService.deleteTopic(topic);
                log.info("删除topic:{}", bool);
            }
        }
    }

    /**
     * 测试删除 topics
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void existTopicName() throws ExecutionException, InterruptedException {
        boolean bool = adminTopicService.existTopicName("Test11");
        log.info("topic是否存在:{}", bool);
    }


    /**
     * 测试获取 topic 的细节
     */
    @Test
    public void getTopic() throws ExecutionException, InterruptedException {
        TopicDescription topicDescription = adminTopicService.getTopicDescription("Test11");
        log.info("topic是否存在:{}", topicDescription);
    }

}
