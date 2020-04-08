package demo.kafka.controller.consume;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.KafkaConsumerService;
import demo.kafka.controller.consume.service.KafkaConsumerSupService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Set;


@Slf4j
@RequestMapping(value = "/ConsumeController")
@RestController
public class ConsumeController {


    /**
     * 把偏移量设置到最早的 offset
     * 这里需要
     * 1.这里会引发再平衡(需要等待一段时间)
     * 2.kafkaManager会有一点延时,但是实际上已经完成
     */
    @ApiOperation(value = "指定消费者的offset设置到最开始", notes = "指定消费者的offset设置到最开始")
    @GetMapping(value = "/seekToBeginning")
    public void seekToBeginning(
            @ApiParam(value = "kafka地址 ", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要调拨的Topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "需要调拨的Topic的消费者groupId")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {

        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id);

        consumerService.subscribe(Arrays.asList(topic));
        consumerService.poll(10);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignments = consumerService.assignment();
        consumerService.seekToBeginning(assignments);
        consumerService.poll(10);//必须要 poll一次才行(不然不会send到server端)
        consumerService.wakeup();
    }

    /**
     *
     */
    @ApiOperation(value = "消费一次", notes = "消费一次")
    @GetMapping(value = "/listenerOnce")
    public void listenerOnce(
            @ApiParam(value = "kafka地址 ", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要消费的的Topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "需要消费的Topic的消费者groupId")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {

        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(bootstrap_servers, group_id, MapUtil.$(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        KafkaConsumerSupService<String, String> kafkaConsumerSupService = KafkaConsumerSupService.getInstance(consumerService);

        kafkaConsumerSupService.listenerOnce(Arrays.asList(topic), consumerRecord -> {
            log.info("offset:{} value:{}", consumerRecord.offset(), consumerRecord.value());
        });
    }


//    @GetMapping(value = "/OffsetAndMetadata")
//    public void OffsetAndMetadata() {
//        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getProducerInstance(Bootstrap.HONE.getIp(), "test");
//        consumerService.subscribe(Arrays.asList("Test11"));
//        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
//        Set<TopicPartition> assignments = consumerService.assignment();
//
//        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
//        assignments.forEach(assignment -> {
//            OffsetAndMetadata offsetAndMetadata = consumerService.committed(assignment);
//            log.info("offsetAndMetadata:{}", offsetAndMetadata);
//        });
//    }

    @GetMapping(value = "/OffsetAndMetadata")
    public void OffsetAndMetadata() {
        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getInstance(Bootstrap.HONE.getIp(), "test");
        consumerService.subscribe(Arrays.asList("Test11"));
        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignments = consumerService.assignment();

        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
        assignments.forEach(assignment -> {
            OffsetAndMetadata offsetAndMetadata = consumerService.committed(assignment);
            log.info("offsetAndMetadata:{}", offsetAndMetadata);
        });
    }


}
