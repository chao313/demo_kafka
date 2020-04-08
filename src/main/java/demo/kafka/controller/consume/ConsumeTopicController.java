package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.consume.service.ConsumerTopicService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;


@Slf4j
@RequestMapping(value = "/ConsumeTopicController")
@RestController
public class ConsumeTopicController {


    /**
     * 根据 topic 来获取 partition
     */
    @ApiOperation(value = "根据消费者来获取 partition")
    @GetMapping(value = "/getPartitionsByTopic")
    public JSONArray getPartitionsByTopic(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "group")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id,
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
        ConsumerTopicService<String, String> consumerTopicService = ConsumerTopicService.getInstance(consumerService);
        Collection<PartitionInfo> partitionsByTopic = consumerTopicService.getPartitionsByTopic(topic);
        String JsonObject = new Gson().toJson(partitionsByTopic);
        JSONArray result = JSONObject.parseArray(JsonObject);
        log.info("获取 TopicConfigs 结果:{}", result);
        return result;
    }

    /**
     * 根据 topic 来获取 partition
     */
    @ApiOperation(value = "获取全部的 partition")
    @GetMapping(value = "/getAllTopicAndPartitions")
    public JSONObject getAllTopicAndPartitions(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "group")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
        ConsumerTopicService<String, String> consumerTopicService = ConsumerTopicService.getInstance(consumerService);
        Map<String, List<PartitionInfo>> topicAndPartitions = consumerTopicService.getAllTopicAndPartitions();
        String JsonObject = new Gson().toJson(topicAndPartitions);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 根据 topic 和 partition 来获取 下一个offset
     */
    @ApiOperation(value = "根据 topic 和 partition 来获取 下一个offset")
    @GetMapping(value = "/getNextOffsetByTopicAndPartition")
    public long getNextOffsetByTopicAndPartition(
            @ApiParam(value = "kafka", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "group")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id,
            String topic,
            int partition

    ) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
        ConsumerTopicService<String, String> consumerTopicService = ConsumerTopicService.getInstance(consumerService);
        long offsetByTopicAndPartition = consumerTopicService.getNextOffsetByTopicAndPartition(topic, partition);
        log.info("下个offset:{}", offsetByTopicAndPartition);
        return offsetByTopicAndPartition;
    }


}
