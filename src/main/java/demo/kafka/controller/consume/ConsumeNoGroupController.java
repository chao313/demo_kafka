package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerFactory;
import demo.kafka.controller.consume.service.ConsumerHavGroupAssignService;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.controller.consume.service.KafkaConsumerCommonService;
import demo.kafka.controller.response.ConsumerTopicAndPartitionsAndOffset;
import demo.kafka.controller.response.TopicPartitionEffectOffsetVo;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.util.*;


@Slf4j
@RequestMapping(value = "/ConsumeNoGroupController")
@RestController
public class ConsumeNoGroupController {


    /**
     * 获取全部的 topic
     */
    @ApiOperation(value = "获取全部的 topic")
    @GetMapping(value = "/getAllTopics")
    public Object getAllTopics(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        Set<String> topics = consumerNoGroupService.getAllTopics();
        return topics;
    }

    /**
     * 根据 topic 来获取 partition
     */
    @ApiOperation(value = "根据消费者来获取 partition")
    @GetMapping(value = "/getPartitionsByTopic")
    public Object getPartitionsByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
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
    public Object getAllTopicAndPartitions(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
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
    public Object getLastPartitionOffsetByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        Map<TopicPartition, Long> offsetByTopicAndPartition = consumerNoGroupService.getLastPartitionOffset(topic);
        String JsonObject = new Gson().toJson(offsetByTopicAndPartition);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 根据 topic 获取每个分区的真实 offset
     */
    @ApiOperation(value = "根据 topic 获取record的最早的 真实有效的偏移量")
    @GetMapping(value = "/getEarliestPartitionOffsetByTopic")
    public Object getEarliestPartitionOffsetByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        Map<TopicPartition, Long> offsetByTopicAndPartition = consumerNoGroupService.getEarliestPartitionOffset(topic);
        String JsonObject = new Gson().toJson(offsetByTopicAndPartition);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 获取 topic 的指定时间戳之后的第一个 offset
     */
    @ApiOperation(value = "获取 topic 的指定时间戳之后的第一个 offset")
    @GetMapping(value = "/getFirstPartitionOffsetAfterTimestamp")
    public Object getFirstPartitionOffsetAfterTimestamp(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            String topic,
            @ApiParam(value = "yyyy-MM-dd HH:mm:ss")
            @RequestParam(name = "dateFormat", defaultValue = "2020-02-22 10:10:10")
                    String dateFormat
    ) throws ParseException {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetBeforeTimestamp
                = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topic,
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(dateFormat).getTime());
        String JsonObject = new Gson().toJson(partitionOffsetBeforeTimestamp);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

    /**
     * 获取partition的详细的信息
     */
    @ApiOperation(value = "获取 partition 的有效的Offset(开始结束)")
    @GetMapping(value = "/getTopicPartitionEffectOffset")
    public Object getTopicPartitionEffectOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition

    ) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService
                = consumerFactory.getConsumerHavGroupAssignService(topic, partition);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long earliestOffset = consumerHavGroupAssignService.getEarliestPartitionOffset(topicPartition);
        Long lastOffset = consumerHavGroupAssignService.getLastPartitionOffset(topicPartition);


        TopicPartitionEffectOffsetVo vo = new TopicPartitionEffectOffsetVo();
        vo.setTopic(topic);
        vo.setPartition(partition);
        vo.setEarliestOffset(earliestOffset);
        vo.setLastOffset(lastOffset);
        String JsonObject = new Gson().toJson(vo);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;

    }


}
