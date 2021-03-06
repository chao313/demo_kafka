package demo.kafka.controller.admin.service;

import demo.kafka.controller.admin.service.base.AdminService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.UnsupportedVersionException;


/**
 * 消费者群组相关
 */
public class AdminConsumerGroupsService extends AdminService {

    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     * {@link AdminFactory#getAdminConsumerGroupsService(String)}
     */
    protected static AdminConsumerGroupsService getInstance(String bootstrap_servers) {
        return new AdminConsumerGroupsService(bootstrap_servers);
    }


    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminConsumerGroupsService(String bootstrap_servers) {
        super(bootstrap_servers);
    }

    /**
     * 查询 消费者群组 的相关信息
     *
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public ListConsumerGroupsResult getConsumerGroups() throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = super.client.listConsumerGroups();
        listConsumerGroupsResult.all().get();
        return listConsumerGroupsResult;
    }


    /**
     * 查询 消费者群组 的相关信息,这个只包含 groipId 和 isSimpleConsumerGroup
     *
     * @return
     */
    public Collection<String> getConsumerGroupIds() throws ExecutionException, InterruptedException {
        Set<String> groupIds = new HashSet<>();
        this.getConsumerGroups().all().get().forEach(consumerGroupListing -> {
            groupIds.add(consumerGroupListing.groupId());
        });
        return groupIds;
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
     * !!! 可以获取每个消费者成员订阅的数据
     */
    public ConsumerGroupDescription getConsumerGroupDescribe(String groupId) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult = super.client.describeConsumerGroups(Arrays.asList(groupId));
        Map<String, ConsumerGroupDescription> stringConsumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();
        return stringConsumerGroupDescriptionMap.get(groupId);
    }

    /**
     * 根据 groupids 获取详细描述 是map
     * 参考 {@link #getConsumerGroupDescribe(String)}
     * <p>
     */
    public Map<String, ConsumerGroupDescription> getConsumerGroupDescribe(Collection<String> groupIds) throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult = super.client.describeConsumerGroups(groupIds);
        Map<String, ConsumerGroupDescription> stringConsumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();
        return stringConsumerGroupDescriptionMap;
    }

    /**
     * 根据 groupid 获取组的成员 和 每个成员订阅的主题
     */
    public Collection<MemberDescription> getConsumerMembersAndSubscribeTopicsByGroupId(String groupId) throws ExecutionException, InterruptedException {
        return this.getConsumerGroupDescribe(groupId).members();
    }

    /**
     * 根据 groupid 获取订阅的TopicPartition
     * 只有客户端没有close才行(正在被订阅的)
     */
    public Set<TopicPartition> getConsumerSubscribedTopicsByGroupId(String groupId) throws ExecutionException, InterruptedException {
        Set<TopicPartition> set = new HashSet<>();
        this.getConsumerGroupDescribe(groupId).members().forEach(memberDescription -> {
            set.addAll(memberDescription.assignment().topicPartitions());
        });
        return set;
    }

    /**
     * 根据 groupId 获取偏移量
     *
     * @return topic的主题的指定分区的偏移量
     * key:TP_01009404-0  value:OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}
     * @throws UnsupportedVersionException The broker only supports OffsetFetchRequest v1, but we need v2 or newer to request all topic partitions.
     */
    public Map<TopicPartition, OffsetAndMetadata> getConsumerGroupOffsets(String groupId) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = super.client.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
        return topicPartitionOffsetAndMetadataMap;
    }

    /**
     * 根据 groupId 获取偏移量()
     * ！！！ 这里只有一个
     *
     * @param listConsumerGroupOffsetsOptions 参数，包含需要获取的partition
     *                                        <p></p>
     * @return topic的主题的指定分区的偏移量
     * key:TP_01009404-0  value:OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}
     * @throws UnsupportedVersionException The broker only supports OffsetFetchRequest v1, but we need v2 or newer to request all topic partitions.
     */
    public Map<TopicPartition, OffsetAndMetadata> getConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions) throws ExecutionException, InterruptedException {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = super.client.listConsumerGroupOffsets(groupId, listConsumerGroupOffsetsOptions);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;
        try {
            topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();

        } catch (Exception e) {
            e.printStackTrace();
            topicPartitionOffsetAndMetadataMap = new HashMap<>();
            topicPartitionOffsetAndMetadataMap.put(listConsumerGroupOffsetsOptions.topicPartitions().get(0), new OffsetAndMetadata(0));
        }
        return topicPartitionOffsetAndMetadataMap;
    }


    /**
     * 根据 groupid 来删除这个消费者
     * 最后会查询一次是否包含
     * <p>
     * -> 删除之后(kafkaManager重新配置就会发现消费者deleted了，再次使用就是新的偏移量)
     *
     * @return
     * @throws ExecutionException: 版本不支持操作 org.apache.kafka.common.errors.UnsupportedVersionException: The broker does not support DELETE_GROUPS
     * @throws ExecutionException: group正在被操作 org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.
     */
    public boolean deleteConsumerGroup(String groupId) throws ExecutionException, InterruptedException {
        DeleteConsumerGroupsResult deleteConsumerGroupsResult = client.deleteConsumerGroups(Arrays.asList(groupId));
        deleteConsumerGroupsResult.all().get();
        return this.existGroupId(groupId) == false ? true : false;
    }


    /**
     * 查询 是否包含指定的 groupId
     */
    public boolean existGroupId(String groupId) throws ExecutionException, InterruptedException {
        return this.getConsumerGroupIds().contains(groupId);
    }
}

