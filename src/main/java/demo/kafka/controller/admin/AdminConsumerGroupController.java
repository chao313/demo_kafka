package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.admin.service.AdminConsumerGroupsService;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
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
                    String bootstrap_servers)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        Collection<String> consumerGroupIds = adminConsumerGroupsService.getConsumerGroupIds();
        log.info("consumerGroupIds:{}", consumerGroupIds);
        return consumerGroupIds;
    }

    @GetMapping(value = "/getConsumerGroups")
    public Object getConsumerGroups(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        ListConsumerGroupsResult listConsumerGroupsResult = adminConsumerGroupsService.getConsumerGroups();
        log.info("listConsumerGroupsResult:{}", listConsumerGroupsResult);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(listConsumerGroupsResult);
        JSONObject jsonObject = JSONObject.parseObject(JsonObject);
        return jsonObject;
    }

    @GetMapping(value = "/getConsumerGroupDescribe")
    public Object getConsumerGroupDescribe(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    )
            throws ExecutionException, InterruptedException {
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
    )
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        /**
         * 兼容老版本
         * 新版本是一句代码 ： Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);
         */
        //获取订阅的topic
//        Set<TopicPartition> topicPartitions = adminConsumerGroupsService.getConsumerSubscribedTopicsByGroupId(group);
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
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
                    String group)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsService adminConsumerGroupsService = AdminFactory.getAdminConsumerGroupsService(bootstrap_servers);
        boolean isDeletedGroupId = adminConsumerGroupsService.deleteConsumerGroup(group);
        log.info("groupId是否被删除:{}", isDeletedGroupId);
        return isDeletedGroupId;
    }
}

