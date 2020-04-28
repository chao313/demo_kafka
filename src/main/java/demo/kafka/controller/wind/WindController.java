package demo.kafka.controller.wind;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceSendSyncService;
import demo.kafka.controller.produce.service.ProduceFactory;
import demo.kafka.controller.response.RecordMetadataResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * 用于kafka生产者
 */
@Slf4j
@RequestMapping(value = "/WindController")
@RestController
public class WindController {

    private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");


    /**
     * 同步! 发送立刻得到结果
     * <p>
     * {
     * "offset": 46,
     * "timestamp": 1585982277534,
     * "serializedKeySize": 5,
     * "serializedValueSize": 5,
     * "partition": 0,
     * "topic": "Topic11"
     * }
     * 专门为wind提供的，上传原始文件
     */
    @ApiOperation(value = "同步! 专门为wind提供的，上传原始文件", notes = "专门为wind提供的，上传原始文件")
    @PostMapping(value = "/sendFile")
    public Object sendFile(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "topic", required = true)
                    String topic,
            @ApiParam(value = "指定 partition -> 不指定就是null")
            @RequestParam(name = "partition", defaultValue = "")
                    Integer partition,
            @RequestParam(name = "policyID", required = true)
                    String policyID,
            @RequestPart(value = "files")
                    MultipartFile[] files
    ) throws ExecutionException, InterruptedException, IOException {
        List<RecordMetadataResponse> recordMetadataResponses = new ArrayList<>();
        KafkaProduceSendSyncService<String, String> kafkaProduceSendSyncService =
                ProduceFactory.getProducerInstance(bootstrap_servers)
                        .getKafkaProduceSendSyncService();
        for (MultipartFile file : files) {/**生成请求*/
            KafkaMsgRequest kafkaMsgRequest = ProducerUpload.generateKafkaRequestMsg(policyID, topic, file.getOriginalFilename(), file.getBytes());
            String key = fastDateFormat.format(new Date());//以上传时间为key
            RecordMetadataResponse recordMetadataResponse
                    = kafkaProduceSendSyncService.sendSync(topic, partition, file.getOriginalFilename() + "_" + key, kafkaMsgRequest.encodeData());
            recordMetadataResponses.add(recordMetadataResponse);
        }
        kafkaProduceSendSyncService.getKafkaProducer().close();
        return recordMetadataResponses;
    }
}

















