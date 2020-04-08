package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceSendForgetService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

/**
 * 专门测试 send 发送就忘记
 */
@Slf4j
public class KafkaProduceServiceSendForgetTest {


    KafkaProduceSendForgetService<String, String> kafkaProduceService = KafkaProduceSendForgetService.getInstance(KafkaProduceSendForgetService.getInstance(Bootstrap.HONE.getIp()));

    /**
     * 测试 发送 value
     */
    @Test
    public void sendForgetTopic_Value() throws InterruptedException {
        kafkaProduceService.sendForget("Test", "1");
        kafkaProduceService.getKafkaProducer().close(Duration.ofDays(1000));
        Thread.sleep(5000);//等待发送完成
    }

    /**
     * 测试 发送 key value
     */
    @Test
    public void sendForgetTopic_Key_Value() throws InterruptedException {
        kafkaProduceService.sendForget("Test", "1", "1");
        kafkaProduceService.getKafkaProducer().close(Duration.ofDays(1000));
        Thread.sleep(5000);//等待发送完成
    }

    /**
     * 测试 发送 key value 到指定的 partition
     */
    @Test
    public void sendForgetPartition_Topic_Key_Value() throws InterruptedException {
        kafkaProduceService.sendForget("Test", 1, "1", "1");
        kafkaProduceService.getKafkaProducer().close(Duration.ofDays(1000));
        Thread.sleep(5000);//等待发送完成
    }

    /**
     * 测试 发送 key value 到指定的 partition
     * 同时指定 时间戳
     */
    @Test
    public void sendForgetTimestamp_Partition_Topic_Key_Value() throws InterruptedException {
        kafkaProduceService.sendForget("Test", 1, new Date().getTime(), "1", "1");
        kafkaProduceService.getKafkaProducer().close(Duration.ofDays(1000));
        Thread.sleep(5000);//等待发送完成
    }


}
