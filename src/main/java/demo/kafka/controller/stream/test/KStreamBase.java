package demo.kafka.controller.stream.test;

import demo.kafka.controller.admin.test.Bootstrap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;

import java.util.Properties;

public class KStreamBase {

    protected static Properties props = new Properties();

    @BeforeAll
    public static void init() {

        /**
         * 指定aplication.id -> 每个Streams的应用程序必须有一个应用ID
         * 1.这个ID用于协调应用实例 和 命名内部存储和相关主题
         * 2.Streams在同一个kafka集群中的id是唯一的
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aplication.id");
        /**
         * 指定kafka的地址
         * 1.Streams也把kafka作为协调工具
         */
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.DEV_WIND.getIp());
        /**
         * 指定 key 和 value 的序列化
         * 1.读写数据时，应用程序需要对消息进行序列化和反序列化 -> 提供了默认的序列化和反序列化的类
         * 2.如果有必要，在创建拓扑时，覆盖默认的类
         */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        /**
         * 指定 auto.offset.reset
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /**
         * 不要再大型生产中使用，会增加网络负载
         */
        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        //props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

    }

}
