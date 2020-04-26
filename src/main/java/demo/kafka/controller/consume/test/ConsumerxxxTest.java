package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerFactory;
import demo.kafka.controller.consume.service.KafkaConsumerCommonService;
import demo.kafka.controller.consume.service.KafkaConsumerSupService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class ConsumerxxxTest {

    public static KafkaConsumer<String, String> kafkaConsumer;
    public static KafkaConsumerSupService<String, String> kafkaConsumerSupService;

    static {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(Bootstrap.HONE.getIp(), "common_imp_db_test");
        kafkaConsumer = consumerFactory.getKafkaConsumer();
        kafkaConsumerSupService = KafkaConsumerSupService.getInstance(consumerFactory.getKafkaConsumer());
    }

    /**
     * 测试 订阅 topic
     */
    @Test
    public void subscribe() {
        kafkaConsumer.subscribe(Pattern.compile(".*"));
        kafkaConsumer.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<String> topics = kafkaConsumer.subscription();
        log.info("已经订阅的 topic:{}", topics);
    }


    /**
     * 测试 获取 topic 的 PartitionInfo
     */
    @Test
    public void getPartitionInfosByTopic() {
        Collection<PartitionInfo> partitionInfos = kafkaConsumerSupService.getPartitionInfosByTopic("Test11");
        partitionInfos.forEach(partitionInfo -> {
            log.info("partitionInfo:{}", partitionInfo);
        });

    }

    /**
     * 测试 获取 topic 的 PartitionInfo
     */
    @Test
    public void getTopicPartitionsByTopic() {
        Collection<TopicPartition> topicPartitions = kafkaConsumerSupService.getTopicPartitionsByTopic("Test11");
        topicPartitions.forEach(topicPartition -> {
            log.info("partitionInfo:{}", topicPartition);
        });

    }

    /**
     * 测试 seekToBeginning
     */
    @Test
    public void seekToBeginning() {

        Collection<TopicPartition> topicPartitions = kafkaConsumerSupService.getTopicPartitionsByTopic("Test");
        topicPartitions.forEach(topicPartition -> {
            log.info("partitionInfo:{}", topicPartition);
        });
        kafkaConsumer.subscribe(Arrays.asList("Test"));
        kafkaConsumer.poll(0);
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        kafkaConsumer.seekToBeginning(assignment);
        kafkaConsumer.commitSync();
    }


    /**
     * 测试 seekToBeginning
     */
    @Test
    public void seekToBeginning_Assign() {

        Collection<TopicPartition> topicPartitions = kafkaConsumerSupService.getTopicPartitionsByTopic("Test");
        topicPartitions.forEach(topicPartition -> {
            log.info("partitionInfo:{}", topicPartition);
        });
        kafkaConsumer.assign(topicPartitions);
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        kafkaConsumer.seekToBeginning(assignment);
        kafkaConsumer.commitSync();
    }

    /**
     * 测试 获取 topic 的 PartitionInfo
     */
    @Test
    public void assignment() {
        Collection<TopicPartition> topicPartitions = kafkaConsumerSupService.getTopicPartitionsByTopic("Test11");
        topicPartitions.forEach(topicPartition -> {
            log.info("partitionInfo:{}", topicPartition);
        });
        kafkaConsumer.assign(topicPartitions);
        kafkaConsumer.poll(0);
        topicPartitions.forEach(topicPartition -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(topicPartition);
            log.info("offsetAndMetadata:{}", offsetAndMetadata);
        });

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
        kafkaConsumer.subscribe(Arrays.asList("Test11"));//订阅一个topic
//        this.poll(consumerService);
    }

    /**
     * 测试监听器
     */
    @Test
    public void listener() {

//        kafkaConsumerSupService.listener(Arrays.asList("Test11"), new Consumer<ConsumerRecords<String, String>>() {
//            @Override
//            public void accept(ConsumerRecords<String, String> consumerRecords) {
//                consumerService.subscribe(Arrays.asList("Test11"));
//                consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
//                Set<TopicPartition> assignments = consumerService.assignment();
//                assignments.forEach(assignment -> {
//                    OffsetAndMetadata offsetAndMetadata = consumerService.committed(assignment);
//                    log.info("offsetAndMetadata:{}", offsetAndMetadata);
//                });
//                consumerRecords.forEach(record -> {
//                    log.info("record 的 offset:{},record:{}", record.offset(), record);
//                });
//                assignments.forEach(assignment -> {
//                    OffsetAndMetadata offsetAndMetadata = consumerService.committed(assignment);
//                    log.info("offsetAndMetadata:{}", offsetAndMetadata);
//                });
//            }
//        });
    }

    /**
     * 测试 订阅 topic
     */
    @Test
    public void subscribexx() {
        kafkaConsumer.subscribe(Arrays.asList("Test11"));
        kafkaConsumer.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignments = kafkaConsumer.assignment();
        log.info("已经订阅的 assignment:{}", assignments);
        assignments.forEach(assignment -> {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(assignment);
            log.info("offsetAndMetadata:{}", offsetAndMetadata);
        });

        kafkaConsumer.seekToBeginning(assignments);
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(0);
        this.listener();

    }
//                    log.info("record 的 topic:{}", record.topic());
//                    log.info("record 的 key:{}", record.key());
//                    log.info("record 的 value:{}", record.value());
//                    log.info("record 的 offset:{}", record.offset());
//                    log.info("record 的 partition:{}", record.partition());
//                    log.info("record 的 serializedKeySize:{}", record.serializedKeySize());
//                    log.info("record 的 serializedValueSize:{}", record.serializedValueSize());
//                    log.info("record 的 timestamp:{}", record.timestamp());
//                    log.info("record 的 timestampType:{}", record.timestampType());
//                    log.info("record 的 headers:{}", record.headers());
//                    log.info("record 的 leaderEpoch:{}", record.leaderEpoch());

    @Test
    public void getOneRecord() {
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        ConsumerRecord oneRecord = consumerCommonService.getOneRecord(Bootstrap.HONE_IP, new TopicPartition("Test", 0), 0);
        ConsumerRecord lastOneRecord = consumerCommonService.getOneRecord(Bootstrap.HONE_IP, new TopicPartition("Test", 0), 53);
        log.info("oneRecord:{}", oneRecord);
        log.info("lastOneRecord:{}", lastOneRecord);
    }


}
