package demo.kafka.controller.stream;

import demo.kafka.controller.admin.test.Bootstrap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    @Test
    public void execute() throws Exception {

        Properties props = new Properties();
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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
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

        StreamsBuilder builder = new StreamsBuilder();

//        builder.

        /**
         * 创建一个stream，定义一个流，指向主题
         *
         */
        KStream<String, String> kStream = builder.stream("Test");

        final Pattern pattern = Pattern.compile("\\W+");

        KStream kStream2 = kStream.filter(
                new Predicate<String, String>() {
                    /**
                     * --> 过滤出符合要求的 key 和 value
                     * 判断给定的key-value是否满足断言
                     * Test if the record with the given key and value satisfies(满足) the predicate(断言).
                     *
                     * @param key    the key of the record
                     * @param value  the value of the record
                     * @return return {@code true} if the key-value pair satisfies the predicate&mdash;{@code false} otherwise
                     */
                    @Override
                    public boolean test(String key, String value) {
                        return value.contains("value");
                    }
                }).map(
                new KeyValueMapper<String, String, KeyValue<String, String>>() {
                    /**
                     * 使用给定的 key 和 value 生成一个新的值
                     * Map a record with the given key and value to a new value.
                     *
                     * @param key    the key of the record
                     * @param value  the value of the record
                     * @return the new value
                     */
                    @Override
                    public KeyValue<String, String> apply(String key, String value) {
                        return new KeyValue<String, String>(key, value);
                    }
                });

        /**
         * 把流的数据写入到指定topic
         */
        kStream2.to("output");

        /**
         * 基于拓扑和配置属性定义一个kafkaStreams对象
         */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();
        /**
         * 开始启动流处理
         */
        streams.start();

        /**
         * 这个流处理实例会永远的运行
         * 这个例子中，只需要运行一点时间，因为输入数据很少
         */
        // usually the streamToTopic application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(5000L);

        streams.close();

    }
}
