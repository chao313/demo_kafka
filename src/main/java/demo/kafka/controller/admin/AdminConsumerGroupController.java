package demo.kafka.controller.admin;


import demo.kafka.controller.admin.util.AdminConsumerGroupsUtil;
import demo.kafka.controller.response.ListConsumerGroupsResultResponse;
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
    public Collection<String> getConsumerGroupIds(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsUtil adminConsumerGroupsUtil = AdminConsumerGroupsUtil.getInstance(bootstrap_servers);
        Collection<String> consumerGroupIds = adminConsumerGroupsUtil.getConsumerGroupIds();
        log.info("consumerGroupIds:{}", consumerGroupIds);
        return consumerGroupIds;
    }

    @GetMapping(value = "/getConsumerGroups")
    public ListConsumerGroupsResultResponse getConsumerGroups(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsUtil adminConsumerGroupsUtil = AdminConsumerGroupsUtil.getInstance(bootstrap_servers);
        ListConsumerGroupsResult listConsumerGroupsResult = adminConsumerGroupsUtil.getConsumerGroups();
        log.info("listConsumerGroupsResult:{}", listConsumerGroupsResult);
        return new ListConsumerGroupsResultResponse(listConsumerGroupsResult);
    }

    @GetMapping(value = "/getConsumerGroupDescribe")
    public ConsumerGroupDescription getConsumerGroupDescribe(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    )
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsUtil adminConsumerGroupsUtil = AdminConsumerGroupsUtil.getInstance(bootstrap_servers);
        ConsumerGroupDescription consumerGroupDescription = adminConsumerGroupsUtil.getConsumerGroupDescribe(group);
        log.info("listConsumerGroupsResult:{}", consumerGroupDescription);
        return consumerGroupDescription;
    }

    @GetMapping(value = "/getConsumerGroupOffsets")
    public Map<TopicPartition, OffsetAndMetadata> getConsumerGroupOffsets(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    )
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsUtil adminConsumerGroupsUtil = AdminConsumerGroupsUtil.getInstance(bootstrap_servers);
        Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsUtil.getConsumerGroupOffsets(group);
        log.info("listConsumerGroupsResult:{}", metadataMap);
        return metadataMap;
    }


    @ApiOperation(value = "删除指定的 consumerGroup")
    @DeleteMapping(value = "/deleteConsumerGroup")
    public boolean deleteConsumerGroup(
            @ApiParam(value = "需要删除的 kafka地址 ", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group)
            throws ExecutionException, InterruptedException {
        AdminConsumerGroupsUtil adminConsumerGroupsUtil = AdminConsumerGroupsUtil.getInstance(bootstrap_servers);
        boolean isDeletedGroupId = adminConsumerGroupsUtil.deleteConsumerGroup(group);
        log.info("groupId是否被删除:{}", isDeletedGroupId);
        return isDeletedGroupId;
    }
}

