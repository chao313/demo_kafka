package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.util.AdminConfigsUtil;
import demo.kafka.controller.admin.util.AdminTopicUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.web.bind.annotation.*;

import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping(value = "/AdminConfigController")
@RestController
public class AdminConfigController {


    @ApiOperation(value = "获取 topic 的配置")
    @GetMapping(value = "/getTopicConfigs")
    public JSONObject getTopicConfigs(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) throws Exception {
        AdminConfigsUtil adminConfigsUtil = AdminConfigsUtil.getInstance(bootstrap_servers);
        Config topicConfigs = adminConfigsUtil.getTopicConfigs(topic);
        String JsonObject = new Gson().toJson(topicConfigs);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取 TopicConfigs 结果:{}", result);
        return result;
    }


    @ApiOperation(value = "获取 topic 的配置")
    @GetMapping(value = "/getBrokerConfigs")
    public JSONObject getBrokerConfigs(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "broker.id")
            @RequestParam(name = "broker.id", defaultValue = "0")
                    int broker_id
    ) throws Exception {
        AdminConfigsUtil adminConfigsUtil = AdminConfigsUtil.getInstance(bootstrap_servers);
        Config topicConfigs = adminConfigsUtil.getBrokerConfigs(broker_id);
        String JsonObject = new Gson().toJson(topicConfigs);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取 BrokerConfigs 结果:{}", result);
        return result;
    }


}

