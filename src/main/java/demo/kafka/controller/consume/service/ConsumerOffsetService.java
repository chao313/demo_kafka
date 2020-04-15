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
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.Group;
import java.util.*;

@Slf4j
public class ConsumerOffsetService<K, V> {

//    KafkaConsumerService<K, V> kafkaConsumerService;
//
//    ConsumerOffsetService(KafkaConsumerService<K, V> kafkaConsumerService) {
//        this.kafkaConsumerService = kafkaConsumerService;
//    }
//
//    private ConsumerOffsetService() {
//    }
//
//    public KafkaConsumerService<K, V> getKafkaConsumerService() {
//        return kafkaConsumerService;
//    }

    @Test
    public void xx() {
        this.parseOffset(Bootstrap.HONE_IP, "consumer-test3");
    }


    public static String hashKeyForDisk(String key) {
        String cacheKey;
        try {
            final MessageDigest mDigest = MessageDigest.getInstance("MD5");
            mDigest.update(key.getBytes());
            cacheKey = bytesToHexString(mDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            cacheKey = String.valueOf(key.hashCode());
        }
        return cacheKey;
    }

    private static String bytesToHexString(byte[] bytes) {
        // http://stackoverflow.com/questions/332079
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                sb.append('0');
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    /**
     * 获取指定group的消费状况
     *
     * @param bootstrap_servers
     * @param group_id
     * @return
     */
    public Map parseOffset(String bootstrap_servers, String group_id) {
        Properties props = new Properties();
        /*配置broker*/
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);//服务器ip:端口号，集群用逗号分隔
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);

        int partition = Math.abs(group_id.hashCode() % 50);//确地group对应的partition

        String topic = "__consumer_offsets";
        /**
         * 获取指定主题的所有分区
         */
        consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));//分配分区
        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> records
                        = consumer.poll(100);
                records.forEach(record -> {
                    try {
                        BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                        OffsetAndMetadata value = null;
                        if (null != record.value()) {
                            value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(record.value()));
//                            value = GroupMetadataManager.offsetCommitValue(ByteBuffer.wrap(record.value()));
                        }

                        System.out.println(key);
                        System.out.println(value);
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


        return null;
    }
}
