package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceSendForgetService;
import demo.kafka.controller.produce.service.KafkaProduceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 专门测试 事务
 */
@Slf4j
public class KafkaProduceTransactionTest {

    @BeforeAll
    public static void BeforeAll() {
        Properties kafkaProps = new Properties(); //新建一个Properties对象
        kafkaProps.put("bootstrap.servers", Bootstrap.HONE.getIp());
        kafkaProps.put("retries", "1");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key准备是String -> 使用了内置的StringSerializer
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value准备是String -> 使用了内置的StringSerializer

        /**
         * 开启事务需要
         * !!! 设置了这个以后正常的发送就不可以了
         */
        kafkaProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionId");
        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(kafkaProps);//创建生产者
        KafkaProduceService.kafkaProducer = kafkaProducer;
    }

    KafkaProduceSendForgetService<String, String> kafkaProduceService = new KafkaProduceSendForgetService<>();

    /**
     * 测试 事务
     */
    @Test
    public void sendForgetTopic_Value() throws InterruptedException {
        kafkaProduceService.transactionSend(Arrays.asList(new ProducerRecord<String, String>("Test", 0, "1", "1")));
        Thread.sleep(5000);//等待发送完成
    }


}

