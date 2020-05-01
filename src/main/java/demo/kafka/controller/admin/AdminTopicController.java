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
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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


    @ApiOperation(value = "获取 topic 的数量")
    @GetMapping(value = "/getTopicSize")
    public Object getTopicSize(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Set<String> topicNames = adminTopicService.getTopicNames();
        log.info("获取 topicNames :{}", topicNames);
        return topicNames.size();
    }

    /**
     * 添加包含过滤
     *
     * @param bootstrap_servers
     * @param topicContain
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "获取 topic 的描述")
    @GetMapping(value = "/getTopicsResults")
    public Object getTopicsResults(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topicContain")
            @RequestParam(name = "topicContain", defaultValue = "")
                    String topicContain

    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Collection<TopicListing> topicsResults = adminTopicService.getTopicsResults();
        if (StringUtils.isNotBlank(topicContain)) {
            topicsResults = topicsResults.stream().filter(topicListing -> {
                return topicListing.name().contains(topicContain);
            }).collect(Collectors.toList());
        }
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

    @ApiOperation(value = "获取全部的 TopicDescription ")
    @GetMapping(value = "/getAllTopicDescription")
    public Object getAllTopicDescription(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topicContain")
            @RequestParam(name = "topicContain", defaultValue = "")
                    String topicContain
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Collection<String> topics = adminTopicService.getTopicNames();
        /**
         * 过滤topic
         */
        if (StringUtils.isNotBlank(topicContain)) {
            topics = adminTopicService.getTopicNames().stream().filter(topicName -> {
                return topicName.contains(topicContain);
            }).collect(Collectors.toList());
        }
        Collection<TopicDescription> topicDescriptions = adminTopicService.getTopicDescription(topics);
        String JsonObject = new Gson().toJson(topicDescriptions);
        JSONArray result = JSONObject.parseArray(JsonObject);
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

    @ApiOperation(value = "清除指定的 Topic(先删除后创建)")
    @DeleteMapping(value = "/clearTopic")
    public Object clearTopic(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要删除的 Topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws Exception {
        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        TopicDescription topicDescription = adminTopicService.getTopicDescription(topic);
        AdminConfigsService adminConfigsService = AdminFactory.getAdminConfigsService(bootstrap_servers);
        Config topicConfigs = adminConfigsService.getTopicConfigs(topic);
        boolean bool = adminTopicService.deleteTopic(topic);
        Thread.sleep(10000);//等待确认
        boolean result = adminTopicService.addTopic(topicDescription.name(),
                topicDescription.partitions().size(),
                Short.valueOf("-1"),
                this.transform(topicConfigs.entries()));
        log.info("clear topic:{}", result);
        return bool;
    }

    /**
     * 配置转换
     *
     * @return
     */
    private Map<String, String> transform(Collection<ConfigEntry> configEntries) {
        Map<String, String> map = new HashMap<>();
        configEntries.stream().forEach(configEntry -> {
            map.put(configEntry.name(), configEntry.value());
        });
        return map;
    }

}

