package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminConsumerGroupsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminConsumerGroupsUtilTest {
    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        adminClient = AdminClient.create(properties);
    }


    /**
     * 获取 消费者群组的 的信息
     */
    @Test
    public void listConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = AdminConsumerGroupsUtil.listConsumerGroups(adminClient);
        listConsumerGroupsResult.all().get().forEach(result -> {
            log.info("groupId:{}", result.groupId());
        });

    }

    /**
     * 获取 消费者群组的 的id信息
     */
    @Test
    public void listConsumerGroupIds() throws ExecutionException, InterruptedException {
        Collection<String> groupIds = AdminConsumerGroupsUtil.listConsumerGroupIds(adminClient);
        groupIds.forEach(groupId -> {
            log.info("groupId:{}", groupId);
        });

    }

    /**
     * 测试 groupId是否存在
     */
    @Test
    public void existGroupId() throws ExecutionException, InterruptedException {
        boolean isExistGroupId = AdminConsumerGroupsUtil.existGroupId(adminClient, "common_imp_db_test");
        log.info("groupId是否存在:{}", isExistGroupId);
    }

    /**
     * 测试 是否能够删除 group(目前报异常:不支持版本)
     */
    @Test
    public void deleteConsumerGroups() throws ExecutionException, InterruptedException {
        boolean isDeletedGroupId = AdminConsumerGroupsUtil.deleteConsumerGroups(adminClient, "common_imp_db_test");
        log.info("groupId是否被删除:{}", isDeletedGroupId);
    }

    /**
     * 测试获取 groupId 的描述
     */
    @Test
    public void describeConsumerGroups() throws ExecutionException, InterruptedException {
        ConsumerGroupDescription consumerGroupDescription = AdminConsumerGroupsUtil.describeConsumerGroups(adminClient, "common_imp_db_test");
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
        Map<TopicPartition, OffsetAndMetadata> map = AdminConsumerGroupsUtil.listConsumerGroupOffsets(adminClient, "common_imp_db_test");
        map.forEach((key, value) -> {
            log.info("key:{}", key);
            log.info("value:{}", value);
        });

    }

}
