//package demo.kafka.controller.consume.test;
//
//import demo.kafka.controller.admin.test.Bootstrap;
//import demo.kafka.controller.admin.util.AdminConsumerGroupsService;
//import demo.kafka.controller.consume.service.ConsumerHavGroupAssignService;
//import demo.kafka.controller.consume.service.ConsumerNoGroupService;
//import demo.kafka.controller.consume.service.base.KafkaConsumerService;
//import demo.kafka.util.MapUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.TopicPartition;
//import org.junit.jupiter.api.Test;
//
//import java.util.Map;
//
//@Slf4j
//public class ConsumerNoGroupTest {
//
//    KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(Bootstrap.DEV_WIND.getIp(), MapUtil.$());
//    ConsumerNoGroupService<String, String> consumerNoGroupService = ConsumerNoGroupService.getInstance(consumerService);
//
//    @Test
//    public void change() {
//        consumerNoGroupService.get
//    }
//
//
//}
