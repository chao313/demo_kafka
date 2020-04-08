package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminConsumerGroupsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminConsumerGroupsServiceTest {


    AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(Bootstrap.MY.getIp());


    /**
     * 获取 消费者群组的 的信息
     */
    @Test
    public void listConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = adminConsumerGroupsService.getConsumerGroups();
        listConsumerGroupsResult.all().get().forEach(result -> {
            log.info("groupId:{}", result.groupId());
        });

    }

    /**
     * 获取 消费者群组的 的id信息
     */
    @Test
    public void listConsumerGroupIds() throws ExecutionException, InterruptedException {
        Collection<String> groupIds = adminConsumerGroupsService.getConsumerGroupIds();
        groupIds.forEach(groupId -> {
            log.info("groupId:{}", groupId);
        });

    }

    /**
     * 测试 groupId是否存在
     */
    @Test
    public void existGroupId() throws ExecutionException, InterruptedException {
        boolean isExistGroupId = adminConsumerGroupsService.existGroupId("common_imp_db_test");
        log.info("groupId是否存在:{}", isExistGroupId);
    }

    /**
     * 测试 是否能够删除 group(目前报异常:不支持版本)
     */
    @Test
    public void deleteConsumerGroups() throws ExecutionException, InterruptedException {
        boolean isDeletedGroupId = adminConsumerGroupsService.deleteConsumerGroup("common_imp_db_test");
        log.info("groupId是否被删除:{}", isDeletedGroupId);
    }


    /**
     * 测试 删除所有的消费者
     */
    @Test
    public void deleteAllConsumerGroups() throws ExecutionException, InterruptedException {
        Collection<String> groups = adminConsumerGroupsService.getConsumerGroupIds();
        for (String group : groups) {
            if (!group.contains("Offset")) {
                try {
                    boolean isDeletedGroupId = adminConsumerGroupsService.deleteConsumerGroup(group);
                    log.info("groupId是否被删除:{}", isDeletedGroupId);
                } catch (Exception e) {
                    log.error("e:{}", group, e);
                }

            }
        }

    }

    /**
     * 测试获取 groupId 的描述
     */
    @Test
    public void describeConsumerGroups() throws ExecutionException, InterruptedException {
        ConsumerGroupDescription consumerGroupDescription = adminConsumerGroupsService.getConsumerGroupDescribe("common_imp_db_test");
        log.info("groupId:{}", consumerGroupDescription.groupId());
        log.info("isSimpleConsumerGroup:{}", consumerGroupDescription.isSimpleConsumerGroup());
        log.info("分区选择器: partitionAssignor:{}", consumerGroupDescription.partitionAssignor());
        log.info("当前消费者状态: state:{}", consumerGroupDescription.state());
        log.info("均衡器: coordinator:{}", consumerGroupDescription.coordinator());
        log.info("参与的主题: members:{}", consumerGroupDescription.members());
        consumerGroupDescription.members().forEach(memberDescription -> {
            log.info("参与的主题: memberDescription:{}", memberDescription);
        });
//        consumerGroupDescription.members().forEach(memberDescription -> {
//            log.info("consumerId:{}", memberDescription.consumerId());
//            log.info("clientId:{}", memberDescription.clientId());
//            log.info("host:{}", memberDescription.host());
//            log.info("MemberAssignment 各个主题的偏移量");
//            memberDescription.assignment().topicPartitions().forEach(topicPartition -> {
//                log.info("topic :{}", topicPartition.topic());
//                log.info("partition :{}", topicPartition.partition());
//            });
//
//
//        });


        log.info("authorizedOperations:{}", consumerGroupDescription.authorizedOperations());

    }

    /**
     * 测试获取 groupId 的的消费偏移量
     */
    @Test
    public void listConsumerGroupOffsets() throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> map = adminConsumerGroupsService.getConsumerGroupOffsets("common_imp_db_test");
        map.forEach((key, value) -> {
            log.info("key:{}", key);
            log.info("value:{}", value);
        });

    }

}
