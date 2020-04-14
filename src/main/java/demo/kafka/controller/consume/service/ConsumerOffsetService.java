package demo.kafka.controller.consume.service;

import demo.kafka.controller.admin.test.Bootstrap;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.*;

@Slf4j
public class ConsumerOffsetService<K, V> {

    KafkaConsumerService<K, V> kafkaConsumerService;

    ConsumerOffsetService(KafkaConsumerService<K, V> kafkaConsumerService) {
        this.kafkaConsumerService = kafkaConsumerService;
    }

    private ConsumerOffsetService() {
    }

    public KafkaConsumerService<K, V> getKafkaConsumerService() {
        return kafkaConsumerService;
    }


    public Map parseOffset(String bootstrap_servers) {
        Properties props = new Properties();
        /*配置broker*/
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);//服务器ip:端口号，集群用逗号分隔
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);

        String topic = "__consumer_offsets";
        /**
         * 获取指定主题的所有分区
         */
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        partitionInfos.forEach(partitionInfo -> {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());//新建topic分区
            topicPartitions.add(topicPartition);//加入list
        });
        consumer.assign(topicPartitions);//把topic分区的list分配给消费者
        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> records
                        = consumer.poll(100);
                records.forEach(record -> {
                    try {
                        log.info("主题:{} , 分区:{} , 偏移量:{},key:{},value:{}", record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value());
                        BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                        OffsetAndMetadata value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()));
                        if (null == record.value()) {
                            System.out.println("null");
                            return;
                        }

                        System.out.println(key);
                        System.out.println(value);
                        String topic1 = ((GroupTopicPartition) key.key()).topicPartition().topic();
                        int partition = ((GroupTopicPartition) key.key()).topicPartition().partition();
                        System.out.println(consumer.endOffsets(Arrays.asList(new TopicPartition(topic1, partition))));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            }
        } catch (Exception e) {
            log.info("msg:{}", e.getMessage(), e);
            e.printStackTrace();
        } finally {
            consumer.close();
        }


    }
}
