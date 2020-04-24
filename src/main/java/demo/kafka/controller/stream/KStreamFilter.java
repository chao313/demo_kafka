package demo.kafka.controller.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 专门处理 filter的辅助类
 */
public class KStreamFilter {
    /**
     * 流构造器
     */
    protected static StreamsBuilder builder = new StreamsBuilder();

    /**
     * @param fromTopics   流的来源topic
     * @param keyPattern   key的正则匹配
     * @param valuePattern value的匹配
     * @param toTopic      流的目标topic
     * @param millis       指定的运行时间 millis
     */
    public void filterMatch(Collection<String> fromTopics,
                            Pattern keyPattern,
                            Pattern valuePattern,
                            String toTopic,
                            long millis,
                            Properties props
    ) throws InterruptedException {
        /**
         * 创建一个stream，定义一个流，指向主题
         */
        KStream<String, String> kStream = builder.stream(fromTopics);

        KStream kStream2 = kStream.filter(
                new Predicate<String, String>() {
                    /**
                     * --> 过滤出符合要求的 key 和 value
                     * 判断给定的key-value是否满足断言
                     * Test if the record with the given key and value satisfies(满足) the predicate(断言).
                     */
                    @Override
                    public boolean test(String key, String value) {

                        if (null != keyPattern && null != valuePattern) {
                            return key.matches(keyPattern.pattern()) && value.matches(valuePattern.pattern());
                        }
                        if (null != keyPattern) {
                            return key.matches(keyPattern.pattern());
                        }
                        if (null != valuePattern) {
                            return value.matches(valuePattern.pattern());
                        }
                        /**
                         * 如果都是 null 全部匹配
                         */
                        return true;
                    }
                });

        /**
         * 把流的数据写入到指定topic
         */
        kStream2.to(toTopic);
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
        Thread.sleep(millis);

        streams.close();

    }

}
