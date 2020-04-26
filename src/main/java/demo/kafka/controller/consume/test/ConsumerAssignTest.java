package demo.kafka.controller.consume.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.*;
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


    public static ConsumerHavGroupAssignService<String, String> consumer;

    static {
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(Bootstrap.PROD_WIND.getIp(), MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        consumer = consumerFactory.getConsumerHavGroupAssignService("TP_BDG_TBSYN_TB6772_INCINDEX");
    }

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
