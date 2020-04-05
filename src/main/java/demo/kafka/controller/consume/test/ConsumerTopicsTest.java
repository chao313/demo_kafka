package demo.kafka.controller.consume.test;

import demo.kafka.controller.consume.service.KafkaConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class ConsumerTopicsTest extends BeforeTest {

    public KafkaConsumerService<String, String> consumerService = new KafkaConsumerService<>();

    /**
     * 测试消费者获取topic
     * -> 可以获取全部的 topic 的信息
     */
    @Test
    public void listTopics() {
        Map<String, List<PartitionInfo>> stringListMap = consumerService.listTopics();

        stringListMap.forEach((key, partitionInfos) -> {
            log.info("key:{}", key);
            log.info("partitionInfos:{}", partitionInfos);
        });
    }


    @Test
    public void assignment() {

        KafkaConsumerService.kafkaConsumer.subscribe(Pattern.compile(".*"));
        KafkaConsumerService.kafkaConsumer.poll(Duration.ofSeconds(2));
        Set<TopicPartition> stringListMap = consumerService.assignment();

        stringListMap.forEach(partitionInfo -> {
            log.info("partitionInfos:{}", partitionInfo);
        });
    }

}
