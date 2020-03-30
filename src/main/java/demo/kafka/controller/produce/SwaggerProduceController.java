package demo.kafka.controller.produce;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
public class SwaggerProduceController {

    /**
     * 创建生产者
     *
     * @return
     */
    @ApiOperation(value = "创建生产者", notes = "使用工厂来生成")
    @GetMapping(value = "/createProducer")
    public KafkaProducer createProducer(
            @ApiParam(value = "bootstrap.servers -> kafka地址", required = true, defaultValue = "") @RequestParam("bootstrap.servers") String bootstrap_servers,
            @ApiParam(value = "zookeeper.connect -> zookeeper地址", required = true) @RequestParam("zookeeper.connect") String zookeeper_connect,
            @ApiParam(value = "acks -> 指定了必须要有多少分区副本接收到消息，生产者才会认为消息是写入成功的", required = true) @RequestParam("acks") String acks,
            @ApiParam(value = "retries", required = true) @RequestParam("retries") String retries,
            @ApiParam(value = "key.serializer -> key的序列化", required = true) @RequestParam("key.serializer") String key_serializer,
            @ApiParam(value = "value.serializer -> value的序列化", required = true) @RequestParam("value.serializer") String value_serializer,
            @ApiParam(value = "buffer.memory -> 用来设置生产者内存缓冲大小", required = true) @RequestParam("buffer.memory") String buffer_memory,
            @ApiParam(value = "compression.type -> 用于设置压缩的方式（snappy,gzip,lz4）.默认是没有压缩", required = true) @RequestParam("compression.type") String compression_type,
            @ApiParam(value = "batch.size -> 指定了一个批次可以使用的内存大小", required = true) @RequestParam("batch.size") String batch_size,
            @ApiParam(value = "linger.ms -> 指定了生产者等待多长时间发送批次", required = true) @RequestParam("linger.ms") String linger_ms,
            @ApiParam(value = "client.id -> 任意字符串，服务器用来识别消息的来源", required = true) @RequestParam("client.id") String client_id,
            @ApiParam(value = "max.in.flight.requests.per.connection ->指定了生产者在接收到服务器消息之前可以发送多少消息", required = true) @RequestParam("max.in.flight.requests.per.connection") String max_in_flight_requests_per_connection,
            @ApiParam(value = "timeout ->指定了生产者在接收到服务器消息之前可以发送多少消息", required = true) @RequestParam("timeout") String timeout,
            @ApiParam(value = "max.block.ms ->指定生产者发送请求的大小 可以指能发送单个消息的最大值 可以指单个请求里所有消息的最大值", required = true) @RequestParam("max.block.ms") String max_block_ms,
            @ApiParam(value = "buffer.bytes ->接收和发送的数据包缓冲区大小", required = true) @RequestParam("buffer.bytes") String buffer_bytes

    ) {
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
}
















