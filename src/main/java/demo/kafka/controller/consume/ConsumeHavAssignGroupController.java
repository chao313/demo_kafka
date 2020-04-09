package demo.kafka.controller.consume;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerHavAssignGroupService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;


@Slf4j
@RequestMapping(value = "/ConsumeHavAssignGroupController")
@RestController
public class ConsumeHavAssignGroupController {


    /**
     * 获取全部的 topic
     */
    @ApiOperation(value = "获取全部的 topic")
    @GetMapping(value = "/getAllTopics")
    public long getAllTopics(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic ")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic) {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerHavAssignGroupService<String, String> consumerHavAssignGroupService = ConsumerHavAssignGroupService.getInstance(consumerService, Arrays.asList(topic));
        long nextOffsetByTopicAndPartition = consumerHavAssignGroupService.getNextOffsetByTopicAndPartition(topic, 0);

        return nextOffsetByTopicAndPartition;
    }


//    /**
//     * 根据 topic 和 partition 来获取 下一个offset
//     */
//    @ApiOperation(value = "根据 topic 和 partition 来获取 下一个offset")
//    @GetMapping(value = "/getNextOffsetByTopicAndPartition")
//    public long getNextOffsetByTopicAndPartition(
//            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
//            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
//                    String bootstrap_servers,
//            @ApiParam(value = "group")
//            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
//                    String group_id,
//            String topic,
//            int partition
//
//    ) {
//        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);
//        ConsumerHavAssignGroupService<String, String> consumerHavAssignGroupService = ConsumerHavAssignGroupService.getInstance(consumerService);
//        long offsetByTopicAndPartition = consumerHavAssignGroupService.getNextOffsetByTopicAndPartition(topic, partition);
//        log.info("下个offset:{}", offsetByTopicAndPartition);
//        return offsetByTopicAndPartition;
//    }

}
