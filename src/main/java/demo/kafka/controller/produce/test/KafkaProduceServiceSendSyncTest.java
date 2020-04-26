package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceSendSyncService;
import demo.kafka.controller.produce.service.ProduceFactory;
import demo.kafka.controller.response.RecordMetadataResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * 专门测试 send 同步
 */
@Slf4j
public class KafkaProduceServiceSendSyncTest {

    KafkaProduceSendSyncService<String, String> kafkaProduceService
            = ProduceFactory.getProducerInstance(Bootstrap.HONE.getIp()).getKafkaProduceSendSyncService();

    /**
     * 测试 发送 value
     * 这里的 key的序列号的size:-1 总是-1(因为没有)
     */
    @Test
    public void sendTopic_Value() throws InterruptedException, ExecutionException {
        RecordMetadataResponse recordMetadataResponse = kafkaProduceService.sendSync("Test", "1");
        log.info("recordMetadataResponse:{}", recordMetadataResponse);
        log.info("当前 Msg 的偏移量:{}", recordMetadataResponse.getOffset());
        log.info("当前 Msg 的Partition:{}", recordMetadataResponse.getPartition());
        log.info("当前 Mag 的key的序列号的size:{}", recordMetadataResponse.getSerializedKeySize());
        log.info("当前 Mag 的value的序列号的size:{}", recordMetadataResponse.getSerializedValueSize());
        log.info("当前 Mag 的时间戳:{}", recordMetadataResponse.getTimestamp());
    }

    /**
     * 测试 发送 key value
     */
    @Test
    public void sendTopic_Key_Value() throws InterruptedException, ExecutionException {
        RecordMetadataResponse recordMetadataResponse = kafkaProduceService.sendSync("Test11", "1", "1");
        log.info("recordMetadataResponse:{}", recordMetadataResponse);
        log.info("当前 Msg 的偏移量:{}", recordMetadataResponse.getOffset());
        log.info("当前 Msg 的Partition:{}", recordMetadataResponse.getPartition());
        log.info("当前 Mag 的key的序列号的size:{}", recordMetadataResponse.getSerializedKeySize());
        log.info("当前 Mag 的value的序列号的size:{}", recordMetadataResponse.getSerializedValueSize());
        log.info("当前 Mag 的时间戳:{}", recordMetadataResponse.getTimestamp());
    }


    /**
     * 测试 发送 key value 到指定的 partition
     * 这里看出 :每个partition的offset都是独立维护的（新的partition的offset总是从0开始）
     */
    @Test
    public void sendPartition_Topic_Key_Value() throws InterruptedException, ExecutionException {
        RecordMetadataResponse recordMetadataResponse = kafkaProduceService.sendSync("Test", 1, "1", "1");
        log.info("recordMetadataResponse:{}", recordMetadataResponse);
        log.info("当前 Msg 的偏移量:{}", recordMetadataResponse.getOffset());
        log.info("当前 Msg 的Partition:{}", recordMetadataResponse.getPartition());
        log.info("当前 Mag 的key的序列号的size:{}", recordMetadataResponse.getSerializedKeySize());
        log.info("当前 Mag 的value的序列号的size:{}", recordMetadataResponse.getSerializedValueSize());
        log.info("当前 Mag 的时间戳:{}", recordMetadataResponse.getTimestamp());
    }

    /**
     * 测试 发送 key value 到指定的 partition
     * 同时指定 时间戳
     */
    @Test
    public void sendTimestamp_Partition_Topic_Key_Value() throws InterruptedException, ExecutionException {
        RecordMetadataResponse recordMetadataResponse = kafkaProduceService.sendSync("Test", 1, new Date().getTime(), "1", "1");
        log.info("recordMetadataResponse:{}", recordMetadataResponse);
        log.info("当前 Msg 的偏移量:{}", recordMetadataResponse.getOffset());
        log.info("当前 Msg 的Partition:{}", recordMetadataResponse.getPartition());
        log.info("当前 Mag 的key的序列号的size:{}", recordMetadataResponse.getSerializedKeySize());
        log.info("当前 Mag 的value的序列号的size:{}", recordMetadataResponse.getSerializedValueSize());
        log.info("当前 Mag 的时间戳:{}", recordMetadataResponse.getTimestamp());
    }
}



