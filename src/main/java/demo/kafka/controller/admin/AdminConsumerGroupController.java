package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.admin.util.AdminConsumerGroupsService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;
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
        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
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
        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
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
        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
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
        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
        Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);
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
        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
        boolean isDeletedGroupId = adminConsumerGroupsService.deleteConsumerGroup(group);
        log.info("groupId是否被删除:{}", isDeletedGroupId);
        return isDeletedGroupId;
    }
}

