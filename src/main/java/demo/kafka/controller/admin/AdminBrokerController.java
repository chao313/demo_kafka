package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.util.AdminClusterUtil;
import demo.kafka.controller.admin.util.AdminConfigsUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.Node;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@Slf4j
@RequestMapping(value = "/AdminBrokerController")
@RestController
public class AdminBrokerController {


    @ApiOperation(value = "获取集群中的 Broker 信息")
    @GetMapping(value = "/getBrokersInCluster")
    public JSONArray getBrokersInCluster(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminClusterUtil adminClusterUtil = AdminClusterUtil.getInstance(bootstrap_servers);
        Collection<Node> nodes = adminClusterUtil.getBrokersInCluster();
        String JsonObject = new Gson().toJson(nodes);
        JSONArray result = JSONObject.parseArray(JsonObject);
        log.info("获取 TopicConfigs 结果:{}", result);
        return result;
    }


    @ApiOperation(value = "获取 topic 的配置")
    @GetMapping(value = "/getBrokerConfigs")
    public JSONObject getBrokerConfigs(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
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

