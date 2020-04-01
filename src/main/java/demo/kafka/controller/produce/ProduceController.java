package demo.kafka.controller.produce;

import demo.kafka.controller.produce.vo.CreateProducerRequest;
import demo.kafka.service.ProduceService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * 用于kafka生产者
 */
@RequestMapping(value = "/ProduceController")
@RestController
public class ProduceController {


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
            @ApiParam(name = "bootstrap.servers", value = "kafka地址")
            @RequestParam(defaultValue = "10.200.127.26:9092")
                    String bootstrap_servers,
            @ApiParam(name = "zookeeper.connect", value = "zookeeper地址")
            @RequestParam(defaultValue = "10.200.127.26:2181")
                    String zookeeper_connect,
            @ApiParam(name = "acks", value = "指定了必须要有多少分区副本接收到消息，生产者才会认为消息是写入成功的")
            @RequestParam(defaultValue = "1")
                    String acks,
            @ApiParam(name = "retries", value = "retries")
            @RequestParam(defaultValue = "1")
                    String retries,
            @ApiParam(name = "key.serializer", value = "key的序列化")
            @RequestParam(defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
                    String key_serializer,
            @ApiParam(name = "value.serializer", value = "value.serializer -> value的序列化")
            @RequestParam(defaultValue = "org.apache.kafka.common.serialization.StringSerializer")
                    String value_serializer,
            @ApiParam(name = "buffer.memory", value = "buffer.memory -> 用来设置生产者内存缓冲大小")
            @RequestParam(defaultValue = "104857600")
                    String buffer_memory,
            @ApiParam(name = "max.request.size", value = "max.request.size ->指定生产者发送请求的大小 可以指能发送单个消息的最大值 可以指单个请求里所有消息的最大值")
            @RequestParam(defaultValue = "10485760000000")
                    String max_request_size,
            @ApiParam(name = "compression.type", value = "compression.type -> 用于设置压缩的方式（snappy,gzip,lz4）.默认是没有压缩")
            @RequestParam(defaultValue = "")
                    String compression_type,
            @ApiParam(name = "batch.size", value = "batch.size -> 指定了一个批次可以使用的内存大小")
            @RequestParam(defaultValue = "")
                    String batch_size,
            @ApiParam(name = "linger.ms", value = "linger.ms -> 指定了生产者等待多长时间发送批次")
            @RequestParam(defaultValue = "")
                    String linger_ms,
            @ApiParam(name = "client.id", value = "client.id -> 任意字符串，服务器用来识别消息的来源")
            @RequestParam(defaultValue = "")
                    String client_id,
            @ApiParam(name = "max.in.flight.requests.per.connection", value = "max.in.flight.requests.per.connection ->指定了生产者在接收到服务器消息之前可以发送多少消息")
            @RequestParam(defaultValue = "")
                    String max_in_flight_requests_per_connection,
            @ApiParam(name = "timeout", value = "timeout ->指定了生产者在接收到服务器消息之前可以发送多少消息")
            @RequestParam(defaultValue = "")
                    String timeout,
            @ApiParam(name = "max.block.ms", value = "max.block.ms -指定了send()发送或partitionFor()获取元数据等待时间。当生产者发送缓冲区已满，或者没有获取到元数据时。方法阻塞max.block.ms的时间，超时抛出异常\n")
            @RequestParam(defaultValue = "")
                    String max_block_ms,

            @ApiParam(name = "buffer.bytes", value = "buffer.bytes ->接收和发送的数据包缓冲区大小")
            @RequestParam(defaultValue = "")
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
        ProduceService.kafkaProducer = kafkaProducer;
        return "kafkaProducer初始化成功";
    }


}

















