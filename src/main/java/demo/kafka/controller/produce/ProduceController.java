package demo.kafka.controller.produce;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceService;
import demo.kafka.controller.produce.vo.CreateProducerRequest;
import demo.kafka.controller.produce.vo.RecordMetadataResponse;
import demo.kafka.service.ProduceService;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 用于kafka生产者
 */
@Slf4j
@RequestMapping(value = "/ProduceController")
@RestController
public class ProduceController {

//    @Autowired
//    private KafkaProduceService kafkaProduceService;


    /**
     * 创建生产者
     *
     * @return
     */
    @ApiOperation(value = "创建生产者", notes = "使用工厂来生成")
    @GetMapping(value = "/createProducer2")
    public KafkaProducer createProducer2(CreateProducerRequest createProducerRequest) {
        Properties kafkaProps = new Properties(); //新建一个Properties对象
        kafkaProps.put("bootstrap.servers", "10.200.127.26:9092");
        kafkaProps.put("zookeeper.connect", "10.200.127.26:2181");
        kafkaProps.put("retries", "1");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key准备是String -> 使用了内置的StringSerializer
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value准备是String -> 使用了内置的StringSerializer
        kafkaProps.put("max.request.size", "10485760000000");
        kafkaProps.put("buffer.memory", "104857600");
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(kafkaProps);//创建生产者
        return kafkaProducer;
    }

    /**
     * 创建生产者
     *
     * @return
     */
    @ApiOperation(value = "创建生产者", notes = "使用工厂来生成")
    @GetMapping(value = "/createProducer")
    public String createProducer(
            @ApiParam(value = "kafka地址")
            @RequestParam(name = "bootstrap.servers", defaultValue = "192.168.0.105:9092")
                    String bootstrap_servers,
            @ApiParam(value = "zookeeper地址")
            @RequestParam(name = "zookeeper.connect", defaultValue = "192.168.0.105:2182")
                    String zookeeper_connect,
            @ApiParam(value = "指定了必须要有多少分区副本接收到消息，生产者才会认为消息是写入成功的")
            @RequestParam(name = "acks", defaultValue = "1")
                    String acks,
            @ApiParam(value = "retries")
            @RequestParam(name = "retries", defaultValue = "1")
                    String retries,
            @ApiParam(value = "key的序列化")
            @RequestParam(name = "key.serializer", defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
                    String key_serializer,
            @ApiParam(value = "value.serializer -> value的序列化")
            @RequestParam(name = "value.serializer", defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
                    String value_serializer,
            @ApiParam(value = "buffer.memory -> 用来设置生产者内存缓冲大小")
            @RequestParam(name = "buffer.memory", defaultValue = "104857600")
                    String buffer_memory,
            @ApiParam(value = "max.request.size ->指定生产者发送请求的大小 可以指能发送单个消息的最大值 可以指单个请求里所有消息的最大值")
            @RequestParam(name = "max.request.size", defaultValue = "10485760000000")
                    String max_request_size,
            @ApiParam(value = "compression.type -> 用于设置压缩的方式（snappy,gzip,lz4）.默认是没有压缩")
            @RequestParam(name = "compression.type", defaultValue = "")
                    String compression_type,
            @ApiParam(value = "batch.size -> 指定了一个批次可以使用的内存大小")
            @RequestParam(name = "batch.size", defaultValue = "")
                    String batch_size,
            @ApiParam(value = "linger.ms -> 指定了生产者等待多长时间发送批次")
            @RequestParam(name = "linger.ms", defaultValue = "")
                    String linger_ms,
            @ApiParam(value = "client.id -> 任意字符串，服务器用来识别消息的来源")
            @RequestParam(name = "client.id", defaultValue = "")
                    String client_id,
            @ApiParam(value = "max.in.flight.requests.per.connection ->指定了生产者在接收到服务器消息之前可以发送多少消息")
            @RequestParam(name = "max.in.flight.requests.per.connection", defaultValue = "")
                    String max_in_flight_requests_per_connection,
            @ApiParam(value = "timeout ->指定了生产者在接收到服务器消息之前可以发送多少消息")
            @RequestParam(name = "timeout", defaultValue = "")
                    String timeout,
            @ApiParam(value = "max.block.ms -指定了send()发送或partitionFor()获取元数据等待时间。当生产者发送缓冲区已满，或者没有获取到元数据时。方法阻塞max.block.ms的时间，超时抛出异常\n")
            @RequestParam(name = "max.block.ms", defaultValue = "")
                    String max_block_ms,
            @ApiParam(value = "buffer.bytes ->接收和发送的数据包缓冲区大小")
            @RequestParam(name = "buffer.bytes", defaultValue = "")
                    String buffer_bytes

    ) {
        Properties kafkaProps = new Properties(); //新建一个Properties对象
        if (StringUtils.isNotBlank(bootstrap_servers)) {
            kafkaProps.put("bootstrap.servers", bootstrap_servers);
        }
        if (StringUtils.isNotBlank(zookeeper_connect)) {
            kafkaProps.put("zookeeper.connect", zookeeper_connect);
        }
        if (StringUtils.isNotBlank(acks)) {
            kafkaProps.put("acks", acks);
        }
        if (StringUtils.isNotBlank(retries)) {
            kafkaProps.put("retries", retries);
        }

        if (StringUtils.isNotBlank(key_serializer)) {
            kafkaProps.put("key.serializer", key_serializer);
        }
        if (StringUtils.isNotBlank(value_serializer)) {
            kafkaProps.put("value.serializer", value_serializer);
        }
        if (StringUtils.isNotBlank(buffer_memory)) {
            kafkaProps.put("buffer.memory", buffer_memory);
        }
        if (StringUtils.isNotBlank(max_request_size)) {
            kafkaProps.put("max.request.size", buffer_memory);
        }
        if (StringUtils.isNotBlank(compression_type)) {
            kafkaProps.put("compression.type", compression_type);
        }
        if (StringUtils.isNotBlank(batch_size)) {
            kafkaProps.put("batch.size", batch_size);
        }
        if (StringUtils.isNotBlank(linger_ms)) {
            kafkaProps.put("linger.ms", linger_ms);
        }
        if (StringUtils.isNotBlank(client_id)) {
            kafkaProps.put("client.id", client_id);
        }
        if (StringUtils.isNotBlank(max_in_flight_requests_per_connection)) {
            kafkaProps.put("max.in.flight.requests.per.connection", max_in_flight_requests_per_connection);
        }
        if (StringUtils.isNotBlank(timeout)) {
            kafkaProps.put("timeout", timeout);
        }
        if (StringUtils.isNotBlank(max_block_ms)) {
            kafkaProps.put("max.block.ms", max_block_ms);
        }
        if (StringUtils.isNotBlank(buffer_bytes)) {
            kafkaProps.put("buffer.bytes", buffer_bytes);
        }
        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(kafkaProps);//创建生产者
//        KafkaProduceService.kafkaProducer = kafkaProducer;
        return "kafkaProducer初始化成功";
    }

//
//    /**
//     * 发送就忘记
//     */
//
//    @ApiOperation(value = "发送就忘记 - 不关心是否发生成功")
//    @GetMapping(value = "/sendForget")
//    public String sendForget(String topic, String key, String value) {
//        kafkaProduceService.sendForget(topic, key, value);
//        return "发送完成，不关心结果";
//    }
//
//    /**
//     * 同步! 发送立刻得到结果
//     * <p>
//     * {
//     * "offset": 46,
//     * "timestamp": 1585982277534,
//     * "serializedKeySize": 5,
//     * "serializedValueSize": 5,
//     * "partition": 0,
//     * "topic": "Topic11"
//     * }
//     */
//    @ApiOperation(value = "同步! 发送立刻得到结果", notes = "可以获得msg的所在topic,分区,时间戳,偏移量,序列号的key和value的size")
//    @GetMapping(value = "/sendSync")
//    public RecordMetadataResponse sendSync(String topic, String key, String value) throws ExecutionException, InterruptedException {
//        RecordMetadataResponse recordMetadataResponse = kafkaProduceService.sendSync(topic, key, value);
//        return recordMetadataResponse;
//    }
//
//    /**
//     * 生产者发送
//     *
//     * @return
//     */
//    @ApiOperation(value = "异步! 发送等待回调")
//    @GetMapping(value = "/sendAsync")
//    public String sendAsync(String topic, String key, String value) {
//        kafkaProduceService.sendAsync(topic, key, value, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                log.info("回调成功:{}", new RecordMetadataResponse(metadata), exception);
//            }
//        });
//        return "异步发送成功!";
//    }
//
//    /**
//     */
//    @ApiOperation(value = "获取生产者度量")
//    @GetMapping(value = "/metricGroupNameMap")
//    public Map<String, List<Metric>> metricGroupNameMap() {
//        Map<String, List<Metric>> stringListMap = KafkaProduceService.metricGroupNameMap();
//        stringListMap.forEach((groupName, metrics) -> {
//            log.info("groupName:{}", groupName);
//            metrics.forEach(metric -> {
//                log.info("groupName:{} -> metricName:{} -> value:{}", metric.metricName().group(), metric.metricName().name(), metric.metricValue());
//            });
//        });
//        return stringListMap;
//    }
//
//    /**
//     * 事务提交
//     *
//     * @return
//     */
//    @ApiOperation(value = "事务提交")
//    @GetMapping(value = "/transactionSendForget")
//    public String transactionSendForget(String topic, String key, String value) {
//        kafkaProduceService.transactionSendForget(topic, MapUtil.$(key, value, key + 1, value + 1, key + 2, value + 2, key + 3, value + 3));
//        return "事务提交成功!";
//    }
//
//
//    @PostConstruct
//    public void init() {
//        Properties kafkaProps = new Properties(); //新建一个Properties对象
//        kafkaProps.put("bootstrap.servers", Bootstrap.HONE.getIp());
//        kafkaProps.put("zookeeper.connect", "10.200.127.26:2181");
//        kafkaProps.put("retries", "1");
//        kafkaProps.put("batch.size", "16384");
//        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//key准备是String -> 使用了内置的StringSerializer
//        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value准备是String -> 使用了内置的StringSerializer
//        /**
//         * 开启事务需要
//         * !!! 设置了这个以后正常的发送就不可以了
//         */
//        kafkaProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionId2");
//        kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        KafkaProducer kafkaConsumer = new KafkaProducer<String, String>(kafkaProps);//创建生产者
//        KafkaProduceService.kafkaConsumer = kafkaConsumer;
//    }



}

















