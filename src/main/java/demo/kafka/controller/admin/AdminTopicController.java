package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.util.AdminTopicUtil;
import demo.kafka.controller.response.TopicDescriptionResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.web.bind.annotation.*;

import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping(value = "/AdminTopicController")
@RestController
public class AdminTopicController {


    @ApiOperation(value = "添加 Topic(简单的创建，还有很多个性化的配置)")
    @PostMapping(value = "/addTopic")
    public boolean addTopic(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
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
        AdminTopicUtil adminTopicUtil = AdminTopicUtil.getInstance(bootstrap_servers);
        boolean bool = adminTopicUtil.addTopic(topic, numPartitions, replicationFactor);
        log.info("添加topic:{}", bool);
        return bool;
    }

    @ApiOperation(value = "获取 topic 的的名称")
    @GetMapping(value = "/getTopicNames")
    public Set<String> getTopicNames(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminTopicUtil adminTopicUtil = AdminTopicUtil.getInstance(bootstrap_servers);
        Set<String> topicNames = adminTopicUtil.getTopicNames();
        log.info("获取 topicNames :{}", topicNames);
        return topicNames;
    }

    @ApiOperation(value = "获取 topic 的描述")
    @GetMapping(value = "/getTopicDescription")
    public JSONObject getTopicDescription(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws Exception {
        AdminTopicUtil adminTopicUtil = AdminTopicUtil.getInstance(bootstrap_servers);
        TopicDescription topicDescription = adminTopicUtil.getTopicDescription(topic);
        String JsonObject = new Gson().toJson(topicDescription);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取topic结果:{}", result);
        return result;
    }

    @ApiOperation(value = "删除指定的 Topic ")
    @DeleteMapping(value = "/deleteTopic")
    public boolean deleteTopic(
            @ApiParam(value = "需要删除的 kafka地址 ", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要删除的 Topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws ExecutionException, InterruptedException {
        AdminTopicUtil adminTopicUtil = AdminTopicUtil.getInstance(bootstrap_servers);
        boolean bool = adminTopicUtil.deleteTopic(topic);
        log.info("删除topic:{}", bool);
        return bool;
    }

}

