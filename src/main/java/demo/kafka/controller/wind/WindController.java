package demo.kafka.controller.wind;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceSendSyncService;
import demo.kafka.controller.produce.service.ProduceFactory;
import demo.kafka.controller.response.RecordMetadataResponse;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
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
    @PostMapping(value = "/sendOriginalFile")
    public Object sendOriginalFile(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", required = true)
                    String bootstrap_servers,
            @RequestParam(name = "topic", required = true)
                    String topic,
            @ApiParam(value = "指定 partition -> 不指定就是null")
            @RequestParam(name = "partition", defaultValue = "0")
                    Integer partition,
            @RequestParam(name = "policyID", required = true)
                    String policyID,
            @RequestPart(value = "files")
                    MultipartFile[] files
    ) throws ExecutionException, InterruptedException, IOException {
        this.vaild(bootstrap_servers, topic, partition, policyID, files);
        List<RecordMetadataResponse> recordMetadataResponses = new ArrayList<>();
        KafkaProduceSendSyncService<String, String> kafkaProduceSendSyncService =
                ProduceFactory.getProducerInstance(bootstrap_servers, MapUtil.$(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1073741824"))
                        .getKafkaProduceSendSyncService();
        for (MultipartFile file : files) {/**生成请求*/
            KafkaMsgRequest kafkaMsgRequest = ProducerUpload.generateKafkaRequestMsg(policyID, topic, file.getOriginalFilename(), file.getBytes());
            RecordMetadataResponse recordMetadataResponse
                    = kafkaProduceSendSyncService.sendSync(topic, partition, file.getOriginalFilename(), kafkaMsgRequest.encodeData());
            recordMetadataResponses.add(recordMetadataResponse);
        }
        String JsonObject = new Gson().toJson(recordMetadataResponses);
        JSONArray result = JSONObject.parseArray(JsonObject);
        kafkaProduceSendSyncService.getKafkaProducer().close();
        return result;
    }

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
     * 同步! 专门为wind提供的，上传业务文件(逐条上传)
     */
    @ApiOperation(value = "同步! 专门为wind提供的，上传业务文件(逐条上传)")
    @PostMapping(value = "/sendBusinessFile")
    public Object sendBusinessFile(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", required = true)
                    String bootstrap_servers,
            @RequestParam(name = "topic", required = true)
                    String topic,
            @ApiParam(value = "指定 partition -> 不指定就是null")
            @RequestParam(name = "partition", defaultValue = "0")
                    Integer partition,
            @RequestParam(name = "policyID", required = true)
                    String policyID,
            @RequestPart(value = "files")
                    MultipartFile[] files
    ) throws ExecutionException, InterruptedException, IOException {
        this.vaild(bootstrap_servers, topic, partition, policyID, files);
        List<RecordMetadataResponse> recordMetadataResponses = new ArrayList<>();
        KafkaProduceSendSyncService<String, String> kafkaProduceSendSyncService =
                ProduceFactory.getProducerInstance(bootstrap_servers)
                        .getKafkaProduceSendSyncService();
        for (MultipartFile file : files) {/**生成请求*/
            LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(file.getInputStream()));
            String line = null;
            while ((line = lineNumberReader.readLine()) != null) {
                /**
                 * 逐行发送 kafka -> key为文件名-行数
                 */
                RecordMetadataResponse recordMetadataResponse
                        = kafkaProduceSendSyncService.sendSync(topic,
                        partition,
                        file.getOriginalFilename() + "-" + lineNumberReader.getLineNumber(),
                        line);
                log.info("上传:", line);
                recordMetadataResponses.add(recordMetadataResponse);
            }
        }
        String JsonObject = new Gson().toJson(recordMetadataResponses);
        JSONArray result = JSONObject.parseArray(JsonObject);
        kafkaProduceSendSyncService.getKafkaProducer().close();
        return result;
    }

    private void vaild(String bootstrap_servers, String topic, Integer partition, String policyID, MultipartFile[] files) {
        if (StringUtils.isBlank(bootstrap_servers)) {
            throw new RuntimeException("bootstrap_servers异常:" + bootstrap_servers);
        }
        if (StringUtils.isBlank(topic)) {
            throw new RuntimeException("topic:" + topic);
        }
        if (null == partition) {
            throw new RuntimeException("partition:" + partition);
        }
        if (StringUtils.isBlank(policyID)) {
            throw new RuntimeException("policyID:" + policyID);
        }
        if (files.length == 0) {
            throw new RuntimeException("files 长度为0 :" + files.length);
        }

    }
}

















