package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.ConsumerHavGroupAssignService;
import demo.kafka.controller.consume.service.KafkaConsumerCommonService;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.controller.consume.service.KafkaConsumerSupService;
import demo.kafka.util.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class ConsumerAssignTest {

    public KafkaConsumerService<String, String> consumerService =
            KafkaConsumerService.getInstance(Bootstrap.PROD_WIND.getIp(), MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1"));

    public ConsumerHavGroupAssignService<String, String> consumer = ConsumerHavGroupAssignService.getInstance(consumerService, "TP_BDG_TBSYN_TB6772_INCINDEX");


    @Test
    public void change() {
        consumer.updatePartitionAssign("TP_BDG_TBSYN_TB6772_INCINDEX");
        consumer.updatePartitionAssignedOffsetToBeginning();
        ConsumerRecords<String, String> consumerRecords = consumer.pollOnce();
        while (!consumerRecords.isEmpty()) {
            consumerRecords.forEach(vo -> {
                log.info("vo:{}->key:{}->value:{}", vo.offset(), vo.key(), vo.value());
            });
        }
    }


}
