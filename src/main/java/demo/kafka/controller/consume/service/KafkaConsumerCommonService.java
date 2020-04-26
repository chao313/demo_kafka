package demo.kafka.controller.consume.service;

import demo.kafka.util.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import scala.collection.immutable.HashMapBuilder;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

/**
 * 增强版(使用组合模式)
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class KafkaConsumerCommonService<K, V> {

    public static final String __consumer_offsets = "__consumer_offsets";


    /**
     * 获取指定的offset record (每个分区的)
     */
    public ConsumerRecord<K, V> getOneRecord(String bootstrap_servers, TopicPartition topicPartition, long offset) {

        /**
         * 获取一个消费者实例
         */

        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        KafkaConsumer<K, V> instance
                = consumerFactory.getKafkaConsumer();
        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));

        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);

        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        if (records.records(topicPartition).size() > 0) {
            return records.records(topicPartition).get(0);
        } else {
            return null;
        }
    }

    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<K, V>> getRecord(String bootstrap_servers,
                                                TopicPartition topicPartition,
                                                long offset,
                                                int recordsNum) {

        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(recordsNum))
        );
        KafkaConsumer<K, V> instance = consumerFactory.getKafkaConsumer();

        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));

        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);

        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        return records.records(topicPartition);
    }

    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<String, String>> getRecord(String bootstrap_servers,
                                                          TopicPartition topicPartition,
                                                          long startOffset,
                                                          long endOffset,
                                                          String keyRegex,
                                                          String valueRegex,
                                                          Long timeStart,
                                                          Long timeEnd

    ) {


        /**
         * 获取一个消费者实例 (设置一次性读取出全部的record)
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(endOffset - startOffset))
        );
        KafkaConsumer<String, String> instance = consumerFactory.getKafkaConsumer();
        /**分配 topicPartition*/
        instance.assign(Arrays.asList(topicPartition));
        /**设置偏移量*/
        instance.seek(topicPartition, startOffset);
        /**获取记录*/
        ConsumerRecords<String, String> records = instance.poll(1000);
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        records.records(topicPartition).forEach(record -> {
            boolean keyRegexFlag = false,
                    valueRegexFlag = false,
                    timeStartFlag = false,
                    timeEndFlag = false;
            if (null == keyRegex) {
                keyRegexFlag = true;
            } else {
                String key = record.key();
                keyRegexFlag = key.matches(keyRegex);
            }
            if (null == valueRegex) {
                valueRegexFlag = true;
            } else {
                String value = record.value();
                valueRegexFlag = value.matches(valueRegex);
            }
            if (null == timeStart) {
                timeStartFlag = true;
            } else {
                long time = record.timestamp();
                timeStartFlag = time > timeStart;
            }
            if (null == timeEnd) {
                timeEndFlag = true;
            } else {
                long time = record.timestamp();
                timeStartFlag = time < timeEnd;
            }
            if (keyRegexFlag && valueRegexFlag && timeStartFlag && timeEndFlag) {
                /**
                 * 全部符合要求
                 */
                result.add(record);
            }
        });


        instance.close();
        return result;
    }


    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<K, V>> getRecord(String bootstrap_servers, TopicPartition topicPartition, long offset, int recordsNum, Map overMap) {

        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(recordsNum))
        );
        KafkaConsumer<K, V> instance = consumerFactory.getKafkaConsumer();
        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));

        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);

        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        return records.records(topicPartition);
    }

}
