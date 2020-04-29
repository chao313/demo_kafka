package demo.kafka.controller.consume.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者的工厂模式
 */
public class ConsumerFactory<K, V> {

    private KafkaConsumer<K, V> kafkaConsumer;

    private ConsumerFactory(KafkaConsumer<K, V> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    private ConsumerFactory() {
    }

    public KafkaConsumer<K, V> getKafkaConsumer() {
        return kafkaConsumer;
    }

    /**
     * 获取无group的 ConsumerService
     */
    public ConsumerNoGroupService<K, V> getConsumerNoGroupService() {
        return ConsumerNoGroupService.getInstance(this.kafkaConsumer);
    }

    /**
     * 获取 分配 的 ConsumerService
     *
     * @return
     */
    public ConsumerHavGroupAssignService<K, V> getConsumerHavGroupAssignService(String topic) {
        return ConsumerHavGroupAssignService.getInstance(this.kafkaConsumer, topic);
    }

    /**
     * 获取 分配 的 ConsumerService
     *
     * @return
     */
    public ConsumerHavGroupAssignService<K, V> getConsumerHavGroupAssignService(TopicPartition topicPartition) {
        return ConsumerHavGroupAssignService.getInstance(this.kafkaConsumer, topicPartition);
    }


    /**
     * 获取 分配 的 ConsumerService
     *
     * @return
     */
    public ConsumerHavGroupAssignService<K, V> getConsumerHavGroupAssignService(KafkaConsumer<K, V> kafkaConsumer, String topic, int partition) {
        return ConsumerHavGroupAssignService.getInstance(this.kafkaConsumer, topic, partition);
    }

    /**
     * 获取 分配 的 ConsumerService
     *
     * @return
     */
    public ConsumerHavGroupAssignService<K, V> getConsumerHavGroupAssignService(String topic, int partition) {
        return ConsumerHavGroupAssignService.getInstance(this.kafkaConsumer, topic, partition);
    }

    /**
     * 获取 订阅 的 ConsumerService
     *
     * @return
     */
    public ConsumerHavGroupSubscribeService<K, V> getConsumerHavGroupSubscribeService(Collection<String> topics) {
        return ConsumerHavGroupSubscribeService.getInstance(this.kafkaConsumer, topics);
    }


    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerFactory<K, V> getInstance(KafkaConsumer kafkaConsumer) {
        return new ConsumerFactory(kafkaConsumer);
    }

    /**
     * 构造函数(注入 kafkaConsumer需要的参数)
     */
    public static <K, V> ConsumerFactory<K, V> getInstance(Properties properties) {
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(properties);
        return new ConsumerFactory(kafkaConsumer);
    }

    /**
     * 构造函数(注入 kafkaConsumer需要的需要的两个参数，其他的默认
     * 默认了  key.deserializer 和 value.deserializer
     */
    public static <K, V> ConsumerFactory<K, V> getInstance(String bootstrap_servers, String group_id) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(props);
        return new ConsumerFactory(kafkaConsumer);
    }

    /**
     * 构造函数(注入 kafkaConsumer需要的需要的两个参数，其他的默认
     * 默认了  key.deserializer 和 value.deserializer
     * !!! 这里填加了mapOver 用作覆盖属性
     */
    public static <K, V> ConsumerFactory<K, V> getInstance(String bootstrap_servers, String group_id, Map<String, String> mapOver) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        props.putAll(mapOver);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(props);
        return new ConsumerFactory(kafkaConsumer);
    }

    /**
     * 获取的是没有group的消费者
     */
    public static <K, V> ConsumerFactory<K, V> getInstance(String bootstrap_servers, Map<String, String> mapOver) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.putAll(mapOver);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(props);
        return new ConsumerFactory(kafkaConsumer);
    }
}
