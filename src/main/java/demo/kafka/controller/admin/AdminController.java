package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.GsonBuilder;
import demo.kafka.controller.admin.service.AdminConsumerGroupsService;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerFactory;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.controller.response.ConsumerGroupOffsetsAndRealOffset;
import demo.kafka.service.RedisService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 综合操作(会调用其他的)
 */
@Slf4j
@RequestMapping(value = "/AdminController")
@RestController
public class AdminController {

    @Autowired
    private RedisTemplate<String, JSONObject> redisTemplateJSONObject;

    @Autowired
    private RedisService redisService;

    @ApiOperation(value = "获取消费偏移量和真实的偏移量")
    @GetMapping(value = "/getConsumerGroupOffsetsAndRealOffset")
    public Object getConsumerGroupOffsetsAndRealOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    ) throws ExecutionException, InterruptedException {

        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();

        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);


        /**
         * 兼容老版本
         * 新版本是一句代码 ： Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);
         */
        //获取订阅的topic
//        Set<TopicPartition> topicPartitions = adminConsumerGroupsService.getConsumerSubscribedTopicsByGroupId(group);
//        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
//        for (TopicPartition partition : topicPartitions) {
//            metadataMap.putAll(adminConsumerGroupsService
//                    .getConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions().topicPartitions(Arrays.asList(partition))));
//        }

        Collection<TopicPartition> allTopicPartitions = consumerNoGroupService.getAllTopicPartitions();
        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
        for (TopicPartition partition : allTopicPartitions) {
            metadataMap.putAll(adminConsumerGroupsService
                    .getConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions().topicPartitions(Arrays.asList(partition))));
        }
        Map<TopicPartition, OffsetAndMetadata> metadataResultMap = new HashMap<>();
        metadataMap.forEach((topicPartition, offsetAndMetadata) -> {
            if (null != offsetAndMetadata) {
                metadataResultMap.put(topicPartition, offsetAndMetadata);
            }
        });
        metadataMap.clear();
        metadataMap.putAll(metadataResultMap);
//        Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);

        Map<TopicPartition, Long> beginningOffsets
                = consumerNoGroupService.getConsumer().beginningOffsets(metadataMap.keySet());
        Map<TopicPartition, Long> endOffsets
                = consumerNoGroupService.getConsumer().endOffsets(metadataMap.keySet());

        /**
         * 处理结果
         */
        Map<TopicPartition, ConsumerGroupOffsetsAndRealOffset> resultOffsetMap = new HashMap<>();//结果
        metadataMap.keySet().forEach(topicPartition -> {
            ConsumerGroupOffsetsAndRealOffset consumerGroupOffsetsAndRealOffset = new ConsumerGroupOffsetsAndRealOffset();
            consumerGroupOffsetsAndRealOffset.setEndOffset(endOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setStartOffset(beginningOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setOffsetAndMetadata(metadataMap.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setTopic(topicPartition.topic());
            consumerGroupOffsetsAndRealOffset.setPartition(topicPartition.partition());
            resultOffsetMap.put(topicPartition, consumerGroupOffsetsAndRealOffset);
        });
        log.info("listConsumerGroupsResult:{}", resultOffsetMap);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(resultOffsetMap);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * @param bootstrap_servers
     * @param topic
     * @param groupNameContain
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @ApiOperation(value = "获取订阅 topic 的 消费者（全部的）")
    @GetMapping(value = "/getConsumerGroupByTopicAndGroupNameContain")
    public Object getConsumerGroupByTopicAndGroupNameContain(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic")
            @RequestParam(name = "topic")
                    String topic,
            @ApiParam(value = "groupNameContain")
            @RequestParam(name = "groupNameContain", defaultValue = "")
                    String groupNameContain
    )
            throws ExecutionException, InterruptedException {
        Set<String> resultGroupIds = new HashSet<>();//结果
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);

        Collection<TopicPartition> topicPartitions = consumerNoGroupService.getTopicPartitionsByTopic(topic);//获取所有的TopicPartition
        Collection<String> consumerGroupIds = adminConsumerGroupsService.getConsumerGroupIds();//查询所有的 consumer;

        /**过滤 group */
        Set<String> filterGroups = new HashSet<>();
        if (StringUtils.isNotBlank(groupNameContain)) {
            consumerGroupIds.forEach(consumerGroupId -> {
                /**包含就是添加*/
                if (consumerGroupId.contains(groupNameContain)) {
                    filterGroups.add(consumerGroupId);
                }
            });
        } else {
            filterGroups.addAll(consumerGroupIds);
        }


        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
        /**
         * 过滤所有的 partition 和 groupId 去尝试是否存在偏移量
         */
        for (TopicPartition partition : topicPartitions) {
            for (String groupId : filterGroups) {
                Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = adminConsumerGroupsService
                        .getConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions().topicPartitions(Arrays.asList(partition)));
                consumerGroupOffsets.forEach((tp, consumerGroupOffset) -> {
                    if (null != consumerGroupOffset) {
                        /**不为空，代表和订阅了这个Topic*/
                        resultGroupIds.add(groupId);
                    }
                });

            }
        }
        /**
         * 获取最终的数据
         */
        Map<String, ConsumerGroupDescription> consumerGroupDescribe
                = adminConsumerGroupsService.getConsumerGroupDescribe(resultGroupIds);


        List<JSONObject> results = new ArrayList<>();
        consumerGroupDescribe.values().forEach(resultGroupNameConsumer -> {
            String JsonObject = new GsonBuilder().serializeNulls().create().toJson(resultGroupNameConsumer);
            JSONObject jsonObject = JSONObject.parseObject(JsonObject);
            results.add(jsonObject);
        });

        String uuid = UUID.randomUUID().toString();
        redisTemplateJSONObject.opsForList().leftPushAll(uuid, results);
        return redisService.getRecordByScrollId(uuid, 1, 20);
    }

    @ApiOperation(value = "获取消费偏移量和真实的偏移量(提供分区)")
    @GetMapping(value = "/getConsumerGroupOffsetsAndRealOffsetByTopicAndPartition")
    public Object getConsumerGroupOffsetsAndRealOffsetByTopicAndPartition(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition
    )
            throws ExecutionException, InterruptedException {

        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();

        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);


        Collection<TopicPartition> allTopicPartitions = Arrays.asList(new TopicPartition(topic, partition));
        Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
        for (TopicPartition topicPartition : allTopicPartitions) {
            metadataMap.putAll(adminConsumerGroupsService
                    .getConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions().topicPartitions(Arrays.asList(topicPartition))));
        }
        Map<TopicPartition, OffsetAndMetadata> metadataResultMap = new HashMap<>();
        metadataMap.forEach((topicPartition, offsetAndMetadata) -> {
            if (null != offsetAndMetadata) {
                metadataResultMap.put(topicPartition, offsetAndMetadata);
            }
        });
        metadataMap.clear();
        metadataMap.putAll(metadataResultMap);

        Map<TopicPartition, Long> beginningOffsets
                = consumerNoGroupService.getConsumer().beginningOffsets(metadataMap.keySet());
        Map<TopicPartition, Long> endOffsets
                = consumerNoGroupService.getConsumer().endOffsets(metadataMap.keySet());

        /**
         * 处理结果
         */
        Map<TopicPartition, ConsumerGroupOffsetsAndRealOffset> resultOffsetMap = new HashMap<>();//结果
        metadataMap.keySet().forEach(topicPartition -> {
            ConsumerGroupOffsetsAndRealOffset consumerGroupOffsetsAndRealOffset = new ConsumerGroupOffsetsAndRealOffset();
            consumerGroupOffsetsAndRealOffset.setEndOffset(endOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setStartOffset(beginningOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setOffsetAndMetadata(metadataMap.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setTopic(topicPartition.topic());
            consumerGroupOffsetsAndRealOffset.setPartition(topicPartition.partition());
            resultOffsetMap.put(topicPartition, consumerGroupOffsetsAndRealOffset);
        });
        log.info("listConsumerGroupsResult:{}", resultOffsetMap);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(resultOffsetMap);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }


}

