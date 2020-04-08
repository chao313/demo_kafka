package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceDefaultService;
import demo.kafka.controller.produce.service.KafkaProduceSendAsyncService;
import demo.kafka.util.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * 专门测试 事务
 */
@Slf4j
public class KafkaProduceTransactionTest {

    KafkaProduceDefaultService<String, String> kafkaProduceDefaultService = KafkaProduceDefaultService.getInstance(
            KafkaProduceSendAsyncService.getProducerInstance(
                    Bootstrap.HONE.getIp(),
                    MapUtil.$(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionId", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"))
    );

    /**
     * 测试 事务
     */
    @Test
    public void sendForgetTopic_Value() throws InterruptedException {
        kafkaProduceDefaultService.transactionSend(Arrays.asList(new ProducerRecord<String, String>("Test", 0, "1", "1")));
        Thread.sleep(5000);//等待发送完成
    }


}

