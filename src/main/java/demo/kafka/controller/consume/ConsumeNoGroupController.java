package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Slf4j
@RequestMapping(value = "/ConsumeNoGroupController")
@RestController
public class ConsumeNoGroupController {


    /**
     * 获取全部的 topic
     */
    @ApiOperation(value = "获取全部的 topic")
    @GetMapping(value = "/getAllTopics")
    public Set<String> getAllTopics(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
        Set<String> topics = consumerNoGroupService.getAllTopics();

        return topics;
    }

    /**
     * 根据 topic 来获取 partition
     */
    @ApiOperation(value = "根据消费者来获取 partition")
    @GetMapping(value = "/getPartitionsByTopic")
    public JSONArray getPartitionsByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "group")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id,
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
        Collection<PartitionInfo> partitionsByTopic = consumerNoGroupService.getPartitionsByTopic(topic);
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
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "group")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
        Map<String, List<PartitionInfo>> topicAndPartitions = consumerNoGroupService.getAllTopicAndPartitions();
        String JsonObject = new Gson().toJson(topicAndPartitions);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }




    /**
     * 根据 topic 获取每个分区的真实 offset
     */
    @ApiOperation(value = "根据 topic 获取每个分区的真实 offset")
    @GetMapping(value = "/getLastPartitionOffsetByTopic")
    public JSONObject getLastPartitionOffsetByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
        Map<TopicPartition, Long> offsetByTopicAndPartition = consumerNoGroupService.getLastPartitionOffsetByTopic(topic);
        String JsonObject = new Gson().toJson(offsetByTopicAndPartition);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 根据 topic 获取每个分区的真实 offset
     */
    @ApiOperation(value = "根据 topic 获取record的最早的 真实有效的偏移量")
    @GetMapping(value = "/getEarliestPartitionOffsetByTopic")
    public JSONObject getEarliestPartitionOffsetByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
        Map<TopicPartition, Long> offsetByTopicAndPartition = consumerNoGroupService.getEarliestPartitionOffsetByTopic(topic);
        String JsonObject = new Gson().toJson(offsetByTopicAndPartition);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 获取 topic 的指定时间戳之后的第一个 offset
     */
    @ApiOperation(value = "获取 topic 的指定时间戳之后的第一个 offset")
    @GetMapping(value = "/getFirstPartitionOffsetAfterTimestamp")
    public JSONObject getFirstPartitionOffsetAfterTimestamp(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic,
            @ApiParam(value = "yyyy-MM-dd HH:mm:ss")
            @RequestParam(name = "dateFormat", defaultValue = "2020-02-22 10:10:10")
                    String dateFormat
    ) throws ParseException {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);

        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetBeforeTimestamp
                = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topic,
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(dateFormat).getTime());
        String JsonObject = new Gson().toJson(partitionOffsetBeforeTimestamp);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }


}
