package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerHavGroupAssignService;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


@Slf4j
@RequestMapping(value = "/ConsumeHavGroupAssignController")
@RestController
public class ConsumeHavGroupAssignController {

    public static ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService;


    /**
     * 获取一个实例
     */
    @ApiOperation(value = "获取一个实例(已经分配好 topic)")
    @GetMapping(value = "/getConsumerHavGroupAssignInstance")
    public String getConsumerHavGroupAssignInstance(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = " 没有偏移量时的重置")
            @RequestParam(name = "auto.offset.reset", defaultValue = "earliest")
                    String auto_offset_reset,
            @ApiParam(value = "一次poll的最大的数量")
            @RequestParam(name = "max.poll.records", defaultValue = "2")
                    String max_poll_records) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, max_poll_records));
        ConsumeHavGroupAssignController.consumerHavGroupAssignService = ConsumerHavGroupAssignService.getInstance(consumerService, topic);
        return "创建ConsumerHavGroupAssignService成功";
    }

    /**
     * 消费一次
     */
    @ApiOperation(value = "pollOnce")
    @GetMapping(value = "/pollOnce")
    public JSONArray pollOnce() {
        List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        consumerHavGroupAssignService.pollOnce(consumerRecord -> {
            log.info("consumerRecord - offset:{} key:{} value:{}", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
            consumerRecords.add(consumerRecord);
        });
        String JsonObject = new Gson().toJson(consumerRecords);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 根据 partition 来获取下一个偏移量
     */
    @ApiOperation(value = "根据 partition 来获取下一个偏移量")
    @GetMapping(value = "/getNextOffsetByTopicAndPartition")
    public long getNextOffsetByTopicAndPartition(
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition
    ) {
        long nextOffsetByTopicAndPartition = consumerHavGroupAssignService.getNextOffsetByTopicAndPartition(topic, partition);
        return nextOffsetByTopicAndPartition;
    }

    /**
     * 查看分配到的Partition
     */
    @ApiOperation(value = "查看分配到的Partition")
    @GetMapping(value = "/getPartitionAssigned")
    public JSONArray getPartitionAssigned() {
        Set<TopicPartition> partitionAssigned = consumerHavGroupAssignService.getPartitionAssigned();
        String JsonObject = new Gson().toJson(partitionAssigned);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * update新的Partition
     * -> 调用之后 {@link #getPartitionAssigned()}  就会改变
     */
    @ApiOperation(value = "查看分配到的Partition")
    @GetMapping(value = "/updatePartitionAssign")
    public JSONArray updatePartitionAssign(
            @ApiParam(value = "被分配的topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic
    ) {
        Collection<TopicPartition> topicPartitionsToBeAssign
                = consumerHavGroupAssignService.updatePartitionAssign(topic);
        String JsonObject = new Gson().toJson(topicPartitionsToBeAssign);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 把订阅到的 partition 全部更新到最开始的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    @ApiOperation(value = "把订阅到的 partition 全部更新到 最开始 的偏移量")
    @GetMapping(value = "/updatePartitionAssignedOffsetToBeginning")
    public JSONArray updatePartitionAssignedOffsetToBeginning() {
        Collection<TopicPartition> partitionToBeSeekBegin
                = consumerHavGroupAssignService.updatePartitionAssignedOffsetToBeginning();
        String JsonObject = new Gson().toJson(partitionToBeSeekBegin);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把订阅到的 partition 全部更新到最新的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     */
    @ApiOperation(value = "把订阅到的 partition 全部更新到 最新 的偏移量")
    @GetMapping(value = "/updatePartitionAssignedOffsetToEnd")
    public JSONArray updatePartitionAssignedOffsetToEnd() {
        Collection<TopicPartition> partitionToBeSeekEnd
                = consumerHavGroupAssignService.updatePartitionAssignedOffsetToEnd();
        String JsonObject = new Gson().toJson(partitionToBeSeekEnd);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }

    /**
     * 把订阅到的 partition 全部更新到 指定的偏移量
     * -> 调用之后 {@link #getNextOffsetByTopicAndPartition(String, int)} 就会改变
     * -> 设置的 offset 超过最大值后，似乎就会从头开始
     */
    @ApiOperation(value = "把订阅到的 partition 全部更新到 指定的偏移量")
    @GetMapping(value = "/updatePartitionAssignedOffset")
    public JSONArray updatePartitionAssignedOffset(
            @ApiParam(value = "指定的 offset")
            @RequestParam(name = "offset", defaultValue = "1")
                    long offset
    ) {
        Collection<TopicPartition> partitionToBeSeek
                = consumerHavGroupAssignService.updatePartitionAssignedOffset(offset);
        String JsonObject = new Gson().toJson(partitionToBeSeek);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 把订阅到的 partition 全部 暂停
     * <p>
     * {@link #pollOnce()} 就会无法获取到值
     */
    @ApiOperation(value = "把订阅到的 partition 全部 暂停")
    @GetMapping(value = "/updatePartitionAssignedToBePause")
    public JSONArray updatePartitionAssignedToBePause() {
        Collection<TopicPartition> partitionToBePause
                = consumerHavGroupAssignService.updatePartitionAssignedToBePause();
        String JsonObject = new Gson().toJson(partitionToBePause);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


    /**
     * 把订阅到的 partition 全部 恢复
     * {@link #pollOnce()} 就会正常获取到值
     */
    @ApiOperation(value = "把订阅到的 partition 全部 恢复")
    @GetMapping(value = "/updatePartitionAssignedToBeResume")
    public JSONArray updatePartitionAssignedToBeResume() {
        Collection<TopicPartition> partitionToBeResume
                = consumerHavGroupAssignService.updatePartitionAssignedToBeResume();
        String JsonObject = new Gson().toJson(partitionToBeResume);
        JSONArray result = JSONObject.parseArray(JsonObject);
        return result;
    }


}
