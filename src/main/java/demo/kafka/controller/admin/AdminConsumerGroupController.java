package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import demo.kafka.controller.admin.service.AdminConsumerGroupsService;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerFactory;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping(value = "/AdminConsumerGroupController")
@RestController
public class AdminConsumerGroupController {


    @GetMapping(value = "/getConsumerGroupIds")
    public Object getConsumerGroupIds(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        Collection<String> consumerGroupIds = adminConsumerGroupsService.getConsumerGroupIds();
        log.info("consumerGroupIds:{}", consumerGroupIds);
        return consumerGroupIds;
    }


    /**
     * 获取全部的描述
     *
     * @param bootstrap_servers
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping(value = "/getConsumerGroups")
    public Object getConsumerGroups(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        Collection<String> consumerGroupIds = adminConsumerGroupsService.getConsumerGroupIds();
        Map<String, ConsumerGroupDescription> groupIdToConsumerGroupDescribes = adminConsumerGroupsService.getConsumerGroupDescribe(consumerGroupIds);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(groupIdToConsumerGroupDescribes.values());//这边只取value
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 获取根据Topic查看消费者(注意只有存活的才行！！！)
     *
     * @param bootstrap_servers
     * @param topic
     * @param groupNameContain  消费者Name包含的字符串
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping(value = "/getAliveConsumerGroupByTopicAndGroupNameContain")
    public Object getAliveConsumerGroupByTopicAndGroupNameContain(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic")
            @RequestParam(name = "topic", defaultValue = "")
                    String topic,
            @ApiParam(value = "groupNameContain")
            @RequestParam(name = "groupNameContain", defaultValue = "")
                    String groupNameContain
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        Collection<String> consumerGroupIds = adminConsumerGroupsService.getConsumerGroupIds();
        Map<String, ConsumerGroupDescription> groupIdToConsumerGroupDescribes = adminConsumerGroupsService.getConsumerGroupDescribe(consumerGroupIds);
        List<ConsumerGroupDescription> filterGroupNameConsumers = new ArrayList<>();
        List<ConsumerGroupDescription> resultGroupNameConsumers = new ArrayList<>();
        groupIdToConsumerGroupDescribes.values().forEach(consumerGroupDescription -> {
            if (StringUtils.isNotBlank(groupNameContain)) {
                /**如果groupNameContain不为blank*/
                if (consumerGroupDescription.groupId().contains(groupNameContain)) {
                    filterGroupNameConsumers.add(consumerGroupDescription);
                }
            } else {
                /**如果groupNameContain为blank -> 添加全部*/
                filterGroupNameConsumers.add(consumerGroupDescription);
            }
        });

        if (StringUtils.isNotBlank(topic)) {
            /**
             * 遍历每个消费者的订阅的Topic -> 过滤
             */
            filterGroupNameConsumers.forEach(consumerGroupDescription -> {
                consumerGroupDescription.members().forEach(memberDescription -> {
                    memberDescription.assignment().topicPartitions().forEach(topicPartition -> {
                        if (topicPartition.topic().equalsIgnoreCase(topic)) {
                            resultGroupNameConsumers.add(consumerGroupDescription);
                        }
                    });
                });
            });
        } else {
            resultGroupNameConsumers.addAll(filterGroupNameConsumers);
        }

        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(resultGroupNameConsumers);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    @GetMapping(value = "/getConsumerGroupDescribe")
    public Object getConsumerGroupDescribe(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        ConsumerGroupDescription consumerGroupDescription = adminConsumerGroupsService.getConsumerGroupDescribe(group);
        log.info("listConsumerGroupsResult:{}", consumerGroupDescription);
        String JsonObject = new Gson().toJson(consumerGroupDescription);
        JSONObject jsonObject = JSONObject.parseObject(JsonObject);
        return jsonObject;
    }

    @GetMapping(value = "/getConsumerGroupOffsets")
    public Object getConsumerGroupOffsets(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        /**
         * 兼容老版本
         * 新版本是一句代码 ： Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);
         */
        //获取订阅的topic
//        Set<TopicPartition> topicPartitions = adminConsumerGroupsService.getConsumerSubscribedTopicsByGroupId(group);
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
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

        log.info("listConsumerGroupsResult:{}", metadataMap);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(metadataMap);
        JSONObject jsonObject = JSONObject.parseObject(JsonObject);
        return jsonObject;
    }


    @ApiOperation(value = "删除指定的 consumerGroup")
    @DeleteMapping(value = "/deleteConsumerGroup")
    public Object deleteConsumerGroup(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    ) throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        boolean isDeletedGroupId = adminConsumerGroupsService.deleteConsumerGroup(group);
        log.info("groupId是否被删除:{}", isDeletedGroupId);
        return isDeletedGroupId;
    }
}

