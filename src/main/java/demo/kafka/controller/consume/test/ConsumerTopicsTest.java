package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@Slf4j
public class ConsumerTopicsTest {

    private static KafkaConsumer<String, String> kafkaConsumer;

    static {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(Bootstrap.HONE.getIp(), "test");
        kafkaConsumer = consumerFactory.getKafkaConsumer();
    }

    /**
     * 测试消费者获取topic
     * -> 可以获取全部的 topic 的信息
     */
    @Test
    public void listTopics() {

        Map<String, List<PartitionInfo>> stringListMap = kafkaConsumer.listTopics();

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
