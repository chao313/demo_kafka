package demo.kafka.controller.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * properties的构建工厂
 */
public class PropertiesFactoryStream {

    static final String earliest = "earliest";

    /**
     * @param application_id      :
     *                            每个Streams的应用程序必须有一个应用ID
     *                            1.这个ID用于协调应用实例 和 命名内部存储和相关主题
     *                            2.Streams在同一个kafka集群中的id是唯一的
     * @param bootstrap_servers   :
     *                            指定kafka的地址
     *                            1.Streams也把kafka作为协调工具
     * @param default_key_serde   :
     *                            指定 key 和 value 的序列化
     *                            1.读写数据时，应用程序需要对消息进行序列化和反序列化 -> 提供了默认的序列化和反序列化的类
     *                            2.如果有必要，在创建拓扑时，覆盖默认的类
     * @param default_value_serde
     * @param auto_offset_reset
     */
    public static Properties create(
            String application_id,
            String bootstrap_servers,
            String default_key_serde,
            String default_value_serde,
            String auto_offset_reset
    ) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, application_id);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, default_key_serde);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, default_value_serde);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset);
        return props;
    }

    /**
     * {@link #create(String, String, String, String, String)} }
     */
    public static Properties create(
            String application_id,
            String bootstrap_servers,
            String auto_offset_reset
    ) {
        return create(application_id, bootstrap_servers, Serdes.String().getClass().getName(), Serdes.String().getClass().getName(), auto_offset_reset);
    }

    /**
     * {@link #create(String, String, String)}
     */
    public static Properties create(
            String application_id,
            String bootstrap_servers
    ) {
        return create(application_id, bootstrap_servers, earliest);
    }
}
