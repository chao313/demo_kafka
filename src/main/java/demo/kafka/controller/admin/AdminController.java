package demo.kafka.controller.admin;


import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.admin.util.AdminConsumerGroupsUtil;
import demo.kafka.controller.admin.util.AdminTopicUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping(value = "/AdminController")
@RestController
public class AdminController {


    @ApiOperation(value = "删除指定的 Topic ")
    @GetMapping(value = "/deleteTopic")
    public boolean deleteTopic(
            @ApiParam(value = "需要删除的 Topic ") @RequestParam(name = "topic", defaultValue = "Test") String topic)
            throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        AdminClient adminClient = AdminClient.create(properties);
        boolean bool = AdminTopicUtil.deleteTopics(adminClient, topic);
        log.info("删除topic:{}", bool);
        return bool;
    }

    @ApiOperation(value = "删除指定的 consumer ")
    @GetMapping(value = "/deleteConsumerGroups")
    public boolean deleteConsumerGroups(
            @ApiParam(value = "需要删除的 group") @RequestParam(name = "group", defaultValue = "common_imp_db_test") String group)
            throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        AdminClient adminClient = AdminClient.create(properties);
        boolean isDeletedGroupId = AdminConsumerGroupsUtil.deleteConsumerGroups(adminClient, group);
        log.info("groupId是否被删除:{}", isDeletedGroupId);
        return isDeletedGroupId;
    }
}

