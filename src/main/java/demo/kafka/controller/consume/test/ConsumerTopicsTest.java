package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.*;

@Slf4j
public class ConsumerTopicsTest {

    KafkaConsumerService<String, String> kafkaConsumerService = KafkaConsumerService.getInstance(Bootstrap.HONE.getIp(), "test");

    /**
     * 测试消费者获取topic
     * -> 可以获取全部的 topic 的信息
     */
    @Test
    public void listTopics() {

        Map<String, List<PartitionInfo>> stringListMap = kafkaConsumerService.listTopics();

        stringListMap.forEach((key, partitionInfos) -> {
            log.info("key:{}", key);
            log.info("partitionInfos:{}", partitionInfos);
        });
    }


//    @Test
//    public void assignment() {
//
//        KafkaConsumerService.kafkaConsumer.subscribe(Pattern.compile(".*"));
//        KafkaConsumerService.kafkaConsumer.poll(Duration.ofSeconds(2));
//        Set<TopicPartition> stringListMap = consumerService.assignment();
//
//        stringListMap.forEach(partitionInfo -> {
//            log.info("partitionInfos:{}", partitionInfo);
//        });
//    }

}
