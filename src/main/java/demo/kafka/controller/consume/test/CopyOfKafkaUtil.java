package demo.kafka.controller.consume.test;


import demo.kafka.controller.admin.test.Bootstrap;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupTopicPartition;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadataManager;

public class CopyOfKafkaUtil {

    static Logger logger = LoggerFactory.getLogger(CopyOfKafkaUtil.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        /*配置broker*/
        props.put("bootstrap.servers", Bootstrap.HONE.getIp());//服务器ip:端口号，集群用逗号分隔
        /*配置从poll(拉)的回话处理时长，以毫秒为单位*/
        props.put("auto.commit.interval.ms", "10");
        /*配置超时时间。，以毫秒为单位*/
        props.put("session.timeout.ms", "300000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
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
                        logger.info("主题:{} , 分区:{} , 偏移量:{},key:{},value:{}", record.topic(),
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
            logger.info("msg:{}", e.getMessage(), e);
            e.printStackTrace();
        } finally {
            consumer.close();
        }


    }

}

