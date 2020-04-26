package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.admin.service.AdminConfigsService;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.service.AdminTopicService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping(value = "/AdminTopicController")
@RestController
public class AdminTopicController {


    @ApiOperation(value = "添加 Topic(简单的创建，还有很多个性化的配置)")
    @PostMapping(value = "/addTopic")
    public Object addTopic(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "topic-分区数量")
            @RequestParam(name = "numPartitions", defaultValue = "1")
                    int numPartitions,
            @ApiParam(value = "topic-复制因子")
            @RequestParam(name = "replicationFactor", defaultValue = "1")
                    short replicationFactor
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        boolean bool = adminTopicService.addTopic(topic, numPartitions, replicationFactor);
        log.info("添加topic:{}", bool);
        return bool;
    }

    @ApiOperation(value = "获取 topic 的的名称")
    @GetMapping(value = "/getTopicNames")
    public Object getTopicNames(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Set<String> topicNames = adminTopicService.getTopicNames();
        log.info("获取 topicNames :{}", topicNames);
        return topicNames;
    }

    @ApiOperation(value = "获取 topic 的的名称")
    @GetMapping(value = "/getTopicsResults")
    public Object getTopicsResults(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Collection<TopicListing> topicsResults = adminTopicService.getTopicsResults();
        String JsonObject = new Gson().toJson(topicsResults);
        JSONArray result = JSONObject.parseArray(JsonObject);
        log.info("获取 topicsResults 结果:{}", result);
        return result;
    }

    @ApiOperation(value = "获取 topic 的描述")
    @GetMapping(value = "/getTopicDescription")
    public Object getTopicDescription(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        TopicDescription topicDescription = adminTopicService.getTopicDescription(topic);
        String JsonObject = new Gson().toJson(topicDescription);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取topic结果:{}", result);
        return result;
    }

    @ApiOperation(value = "删除指定的 Topic ")
    @DeleteMapping(value = "/deleteTopic")
    public Object deleteTopic(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要删除的 Topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws ExecutionException, InterruptedException {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        boolean bool = adminTopicService.deleteTopic(topic);
        log.info("删除topic:{}", bool);
        return bool;
    }

    @ApiOperation(value = "获取 topic 的配置")
    @GetMapping(value = "/getTopicConfigs")
    public Object getTopicConfigs(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws Exception {
        AdminConfigsService adminConfigsService = AdminFactory.getAdminConfigsService(bootstrap_servers);
        Config topicConfigs = adminConfigsService.getTopicConfigs(topic);
        String JsonObject = new Gson().toJson(topicConfigs);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取 TopicConfigs 结果:{}", result);
        return result;
    }

}

