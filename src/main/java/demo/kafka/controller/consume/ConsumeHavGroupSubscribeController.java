package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerHavGroupAssignService;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


@Slf4j
@RequestMapping(value = "/ConsumeHavAssignGroupController")
@RestController
public class ConsumeHavGroupSubscribeController {

    public static ConsumerHavGroupSubscribeService<String, String> consumerHavGroupSubscribeService;

    /**
     * 查看分配到的Partition
     */
    @ApiOperation(value = "查看订阅到的Partition")
    @GetMapping(value = "/getPartitionSubscribed")
    public Collection getPartitionSubscribed() {
        Set<String> partitionSubscribed = consumerHavGroupSubscribeService.getPartitionSubscribed();
        return partitionSubscribed;
    }





}
