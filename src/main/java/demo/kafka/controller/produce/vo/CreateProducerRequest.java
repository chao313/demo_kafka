package demo.kafka.controller.produce.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiParam;
import lombok.Data;
import org.springframework.web.bind.annotation.RequestParam;

@Data
@ApiModel
public class CreateProducerRequest {
    @ApiModelProperty(
            name = "bootstrap.servers",
            value = "bootstrap.servers:kafka地址"
    )
    String bootstrap_servers;

    @ApiModelProperty(
            name = "zookeeper.connect",
            value = "zookeeper.connect:zookeeper地址"
    )
    String zookeeper_connect;

    @ApiModelProperty(
            name = "acks",
            value = "acks指定了必须要有多少分区副本接收到消息，生产者才会认为消息是写入成功的"
    )
    String acks;

    @ApiModelProperty(
            name = "retries",
            value = "retries:重试次数"
    )
    String retries;

    @ApiModelProperty(
            name = "key.serializer",
            value = "key.serializer:key的序列化"
    )
    String key_serializer;

    @ApiModelProperty(
            name = "value.serializer",
            value = "value.serializer:value的序列化")
    String value_serializer;

    @ApiModelProperty(
            name = "buffer.memory",
            value = "buffer.memory:设置生产者内存缓冲大小"
    )
    String buffer_memory;

    @ApiModelProperty(
            name = "compression.type",
            value = "compression.type:设置压缩的方式（snappy,gzip;lz4）.默认是没有压缩"
    )
    String compression_type;

    @ApiModelProperty(
            name = "batch.size",
            value = "batch.size:设置一个批次可以使用的内存大小"
    )
    String batch_size;

    @ApiModelProperty(
            name = "linger.ms",
            value = "linger.ms:设置生产者等待多长时间发送批次")
    String linger_ms;

    @ApiModelProperty(
            name = "client.id",
            value = "client.id:任意字符串，服务器用来识别消息的来源")
    String client_id;

    @ApiModelProperty(
            name = "max.in.flight.request.per.connection",
            value = "max.in.flight.requests.per.connection:指定了生产者在接收到服务器消息之前可以发送多少消息"
    )
    String max_in_flight_requests_per_connection;

    @ApiModelProperty(
            name = "timeout",
            value = "timeout:设置了生产者在接收到服务器消息之前可以发送多少消息"
    )
    String timeout;

    @ApiModelProperty(
            name = "max.block.ms",
            value = "max.block.ms:指定生产者发送请求的大小 可以指能发送单个消息的最大值 可以指单个请求里所有消息的最大值"
    )
    String max_block_ms;

    @ApiModelProperty(
            name = "buffer.bytes",
            value = "buffer.bytes:接收和发送的数据包缓冲区大小"
    )
    String buffer_bytes;
}
