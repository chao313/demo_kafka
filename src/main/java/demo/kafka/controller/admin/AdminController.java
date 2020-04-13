package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.GsonBuilder;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.admin.util.AdminConsumerGroupsService;
import demo.kafka.controller.consume.service.ConsumerNoGroupService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.controller.response.ConsumerGroupOffsetsAndRealOffset;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 综合操作(会调用其他的)
 */
@Slf4j
@RequestMapping(value = "/AdminController")
@RestController
public class AdminController {


    @ApiOperation(value = "获取消费偏移量和真实的偏移量")
    @GetMapping(value = "/getConsumerGroupOffsetsAndRealOffset")
    public Object getConsumerGroupOffsetsAndRealOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group", defaultValue = "common_imp_db_test")
                    String group
    )
            throws ExecutionException, InterruptedException {

        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);

        AdminConsumerGroupsService adminConsumerGroupsService = AdminConsumerGroupsService.getInstance(bootstrap_servers);
        Map<TopicPartition, OffsetAndMetadata> metadataMap = adminConsumerGroupsService.getConsumerGroupOffsets(group);


        Map<TopicPartition, Long> beginningOffsets
                = consumerNoGroupService.getKafkaConsumerService().beginningOffsets(metadataMap.keySet());
        Map<TopicPartition, Long> endOffsets
                = consumerNoGroupService.getKafkaConsumerService().endOffsets(metadataMap.keySet());

        /**
         * 处理结果
         */
        Map<TopicPartition, ConsumerGroupOffsetsAndRealOffset> resultOffsetMap = new HashMap<>();//结果
        metadataMap.keySet().forEach(topicPartition -> {
            ConsumerGroupOffsetsAndRealOffset consumerGroupOffsetsAndRealOffset = new ConsumerGroupOffsetsAndRealOffset();
            consumerGroupOffsetsAndRealOffset.setEndOffset(endOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setStartOffset(beginningOffsets.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setOffsetAndMetadata(metadataMap.get(topicPartition));
            consumerGroupOffsetsAndRealOffset.setTopic(topicPartition.topic());
            consumerGroupOffsetsAndRealOffset.setPartition(topicPartition.partition());
            resultOffsetMap.put(topicPartition, consumerGroupOffsetsAndRealOffset);
        });
        log.info("listConsumerGroupsResult:{}", resultOffsetMap);
        String JsonObject = new GsonBuilder().serializeNulls().create().toJson(resultOffsetMap);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }

}

