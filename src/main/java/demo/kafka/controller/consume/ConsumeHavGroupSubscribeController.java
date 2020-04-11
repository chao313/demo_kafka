package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerHavGroupSubscribeService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;


@Slf4j
@RequestMapping(value = "/ConsumeHavAssignGroupController")
@RestController
public class ConsumeHavGroupSubscribeController {

    public static ConsumerHavGroupSubscribeService<String, String> consumerHavGroupSubscribeService;


    /**
     * 获取一个实例
     */
    @ApiOperation(value = "获取一个实例(已经订阅好 topic)")
    @GetMapping(value = "/getConsumerHavGroupSubscribeInstance")
    public String getConsumerHavGroupSubscribeInstance(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要订阅的topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "group.id")
            @RequestParam(name = "group.id", defaultValue = "consumer-test")
                    String group_id,
            @ApiParam(value = " 没有偏移量时的重置")
            @RequestParam(name = "auto.offset.reset", defaultValue = "earliest")
                    String auto_offset_reset,
            @ApiParam(value = "一次poll的最大的数量")
            @RequestParam(name = "max.poll.records", defaultValue = "2")
                    String max_poll_records) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id,
                MapUtil.$(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max_poll_records)
        );
        if (null != ConsumeHavGroupSubscribeController.consumerHavGroupSubscribeService) {
            /**
             * 如果不为空就关闭
             */
            ConsumeHavGroupSubscribeController.consumerHavGroupSubscribeService.getKafkaConsumerService().close();
        }
        ConsumeHavGroupSubscribeController.consumerHavGroupSubscribeService
                = ConsumerHavGroupSubscribeService.getInstance(consumerService, Arrays.asList(topic));
        return "创建 ConsumerHavGroupSubscribeService 成功";
    }

    /**
     * 消费一次
     */
    @ApiOperation(value = "pollOnce")
    @GetMapping(value = "/pollOnce")
    public Object pollOnce() {
        List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        consumerHavGroupSubscribeService.pollOnce(consumerRecord -> {
            log.info("consumerRecord - offset:{} key:{} value:{}", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
            consumerRecords.add(consumerRecord);
        });
        String JsonObject = new Gson().toJson(consumerRecords);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 查看订阅到的 topic
     */
    @ApiOperation(value = "查看订阅到的 Topic")
    @GetMapping(value = "/getTopicSubscribed")
    public Object getTopicSubscribed() {
        Set<String> topicSubscribed = consumerHavGroupSubscribeService.getTopicSubscribed();
        return topicSubscribed;
    }

    /**
     * update 订阅的主题
     */
    @ApiOperation(value = "update 订阅的主题")
    @GetMapping(value = "/updateTopicSubscribed")
    public Object getTopicSubscribed(
            @ApiParam(value = "需要更新的主题")
            @RequestParam(name = "topics", defaultValue = "Test,Test1")
                    List<String> topics
    ) {
        consumerHavGroupSubscribeService.updateTopicSubscribed(topics);
        return "更新订阅主题成功";
    }


    /**
     * 根据 partition 来获取下一个偏移量
     */
    @ApiOperation(value = "根据 partition 来获取下一个偏移量")
    @GetMapping(value = "/getNextOffsetByTopicAndPartition")
    public Object getNextOffsetByTopicAndPartition(
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition
    ) {
        long nextOffsetByTopicAndPartition = consumerHavGroupSubscribeService.getNextOffsetByTopicAndPartition(topic, partition);
        return nextOffsetByTopicAndPartition;
    }

    /**
     * 把订阅到的 topic 全部更新到最开始的偏移量
     */
    @ApiOperation(value = "把订阅到的 topic 全部更新到最开始的偏移量")
    @GetMapping(value = "/updateTopicSubscribedOffsetToBeginning")
    public Object updateTopicSubscribedOffsetToBeginning() {
        Collection<TopicPartition> topicSubscribedOffsetToBeginning
                = consumerHavGroupSubscribeService.updateTopicSubscribedOffsetToBeginning();
        String JsonObject = new Gson().toJson(topicSubscribedOffsetToBeginning);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把订阅到的 partition 全部更新到最新的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    @ApiOperation(value = "把订阅到的 partition 全部更新到最新的偏移量")
    @GetMapping(value = "/updatePartitionSubscribedOffsetToEnd")
    public Object updatePartitionSubscribedOffsetToEnd() {
        Collection<TopicPartition> partitionToBeSeekBegin
                = consumerHavGroupSubscribeService.updatePartitionSubscribedOffsetToEnd();
        String JsonObject = new Gson().toJson(partitionToBeSeekBegin);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把 订阅 到的 topic 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    @ApiOperation(value = "把 订阅 到的 topic 全部更新到 指定的偏移量")
    @GetMapping(value = "/updatePartitionSubscribedOffset")
    public Object updatePartitionSubscribedOffset(
            @ApiParam(value = "指定的 offset")
            @RequestParam(name = "offset", defaultValue = "1")
                    long offset
    ) {
        Collection<TopicPartition> partitionToBeSeek
                = consumerHavGroupSubscribeService.updatePartitionSubscribedOffset(offset);
        String JsonObject = new Gson().toJson(partitionToBeSeek);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把 订阅 到的 topic 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    @ApiOperation(value = "把 订阅 到的 topic (指定)更新到 指定的偏移量")
    @GetMapping(value = "/updatePartitionSubscribedOffsetByTopics")
    public Object updatePartitionSubscribedOffset(
            @ApiParam(value = "需要更新的主题")
            @RequestParam(name = "topics", defaultValue = "Test,Test1")
                    List<String> topics,
            @ApiParam(value = "指定的 offset")
            @RequestParam(name = "offset", defaultValue = "1")
                    long offset
    ) {
        Collection<TopicPartition> partitionToBeSeek
                = consumerHavGroupSubscribeService.updatePartitionSubscribedOffset(topics, offset);
        String JsonObject = new Gson().toJson(partitionToBeSeek);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把 订阅 到的 partition 全部 暂停
     * <p>
     * {@link #pollOnce()} 就会无法获取到值
     */
    @ApiOperation(value = "把 分配 到的 partition 全部 暂停")
    @GetMapping(value = "/updatePartitionSubscribedToBePause")
    public Object updatePartitionSubscribedToBePause() {
        Collection<TopicPartition> partitionToBePause
                = consumerHavGroupSubscribeService.updatePartitionSubscribedToBePause();
        String JsonObject = new Gson().toJson(partitionToBePause);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 把 订阅 到的 partition 全部 恢复
     * {@link #pollOnce()} 就会正常获取到值
     */
    @ApiOperation(value = "把 分配 到的 partition 全部 恢复")
    @GetMapping(value = "/updatePartitionSubscribedToBeResume")
    public Object updatePartitionSubscribedToBeResume() {
        Collection<TopicPartition> partitionToBeResume
                = consumerHavGroupSubscribeService.updatePartitionSubscribedToBeResume();
        String JsonObject = new Gson().toJson(partitionToBeResume);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

}
