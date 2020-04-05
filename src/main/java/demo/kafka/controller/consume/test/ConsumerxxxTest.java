package demo.kafka.controller.consume.test;

import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.controller.consume.service.KafkaConsumerSupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Slf4j
public class ConsumerxxxTest extends BeforeTest {

    public KafkaConsumerService<String, String> consumerService = new KafkaConsumerService<>();

    /**
     * 测试 订阅 topic
     */
    @Test
    public void subscribe() {
        consumerService.subscribe(Pattern.compile(".*"));
        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<String> topics = consumerService.subscription();
        log.info("已经订阅的 topic:{}", topics);
    }

    /**
     * 测试 普通的 订阅
     * <p>
     * record 的 topic:Test11
     * record 的 key:1
     * record 的 value:1
     * record 的 offset:47
     * record 的 partition:0
     * record 的 serializedKeySize:1
     * record 的 serializedValueSize:1
     * record 的 timestamp:1586088409428
     * record 的 timestampType:CreateTime
     * record 的 headers:RecordHeaders(headers = [], isReadOnly = false)
     * record 的 leaderEpoch:Optional[0]
     */
    @Test
    public void subscribeTest() {
        consumerService.subscribe(Arrays.asList("Test11"));//订阅一个topic
//        this.poll(consumerService);
    }

    /**
     * 测试监听器
     */
    @Test
    public void listener() {
        KafkaConsumerSupService<String, String> kafkaConsumerSupService = new KafkaConsumerSupService<>();
        kafkaConsumerSupService.listener(Arrays.asList("Test11"), new Consumer<ConsumerRecords<String, String>>() {
            @Override
            public void accept(ConsumerRecords<String, String> consumerRecords) {
                consumerRecords.forEach(record -> {
                    log.info("record 的 topic:{}", record.topic());
                    log.info("record 的 key:{}", record.key());
                    log.info("record 的 value:{}", record.value());
                    log.info("record 的 offset:{}", record.offset());
                    log.info("record 的 partition:{}", record.partition());
                    log.info("record 的 serializedKeySize:{}", record.serializedKeySize());
                    log.info("record 的 serializedValueSize:{}", record.serializedValueSize());
                    log.info("record 的 timestamp:{}", record.timestamp());
                    log.info("record 的 timestampType:{}", record.timestampType());
                    log.info("record 的 headers:{}", record.headers());
                    log.info("record 的 leaderEpoch:{}", record.leaderEpoch());
                });
            }
        });
    }

    /**
     * 测试 订阅 topic
     */
    @Test
    public void subscribexx() {
        consumerService.subscribe(Pattern.compile(".*"));
        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignment = consumerService.assignment();
        log.info("已经订阅的 assignment:{}", assignment);
        consumerService.seekToBeginning(assignment);
        consumerService.poll(0);
    }


}
