package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.UnsupportedVersionException;


/**
 * 消费者群组相关
 */
public class AdminConsumerGroupsUtil {

    /**
     * 查询 消费者群组 的相关信息
     *
     * @param client
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static ListConsumerGroupsResult listConsumerGroups(AdminClient client) throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();
        listConsumerGroupsResult.all().get();
        return client.listConsumerGroups();
    }


    /**
     * 查询 消费者群组 的相关信息,这个只包含 groipId 和 isSimpleConsumerGroup
     *
     * @param client
     * @return
     */
    public static Collection<String> listConsumerGroupIds(AdminClient client) throws ExecutionException, InterruptedException {
        Set<String> groupIds = new HashSet<>();
        AdminConsumerGroupsUtil.listConsumerGroups(client).all().get().forEach(consumerGroupListing -> {
            groupIds.add(consumerGroupListing.groupId());
        });
        return groupIds;
    }

    /**
     * 查询 是否包含指定的 groupId
     *
     * @param client
     * @return
     */
    public static boolean existGroupId(AdminClient client, String groupId) throws ExecutionException, InterruptedException {
        return AdminConsumerGroupsUtil.listConsumerGroupIds(client).contains(groupId);
    }


    /**
     * 根据 groupid 来删除这个消费者
     * 最后会查询一次是否包含
     *
     * @param client
     * @return
     * @throws ExecutionException: 版本不支持操作 org.apache.kafka.common.errors.UnsupportedVersionException: The broker does not support DELETE_GROUPS
     * @throws ExecutionException: group正在被操作 org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.
     */
    public static boolean deleteConsumerGroups(AdminClient client, String groupId) throws ExecutionException, InterruptedException {
        DeleteConsumerGroupsResult deleteConsumerGroupsResult = client.deleteConsumerGroups(Arrays.asList(groupId));
        deleteConsumerGroupsResult.all().get();
        return AdminConsumerGroupsUtil.existGroupId(client, groupId) == false ? true : false;
    }

    /**
     * 根据 groupid 获取详细描述 (这里只取了一个)
     * <p>
     *
     * @return ：
     * 1.是否是简单消费者
     * 2.状态(Stable/Dead)
     * 3.均衡器  coordinator
     * 4.分区选择器 partitionAssignor
     * 5.成员:Members
     */
    public static ConsumerGroupDescription describeConsumerGroups(AdminClient client, String groupId) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult = client.describeConsumerGroups(Arrays.asList(groupId));
        Map<String, ConsumerGroupDescription> stringConsumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();
        return stringConsumerGroupDescriptionMap.get(groupId);
    }

    /**
     * 根据 groupId 获取偏移量
     *
     * @return topic的主题的指定分区的偏移量
     * key:TP_01009404-0  value:OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}
     * @throws UnsupportedVersionException The broker only supports OffsetFetchRequest v1, but we need v2 or newer to request all topic partitions.
     */
    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(AdminClient client, String groupId) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = client.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
        return topicPartitionOffsetAndMetadataMap;
    }
}

