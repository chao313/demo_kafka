package demo.kafka.controller.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Collection;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * 专门处理 filter的辅助类
 */
public class KStreamModel<K, V> {
    /**
     * 流构造器
     */
    protected static StreamsBuilder builder = new StreamsBuilder();


    /**
     * 没有终点的操作
     * 使用模板方法 -> 只需要专注于流逻辑的编写
     *
     * @param fromTopics
     * @param millis
     * @param props
     * @param consumer
     * @throws InterruptedException
     */
    public void streamNoTo(Collection<String> fromTopics,
                           long millis,
                           Properties props,
                           Consumer<KStream<K, V>> consumer
    ) throws InterruptedException {
        /**
         * 创建一个stream，定义一个流，指向主题
         */
        KStream<K, V> kStream = builder.stream(fromTopics);


        consumer.accept(kStream);

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


    /**
     * 这个终点在topic
     * 使用模板方法 -> 只需要专注于流逻辑的编写
     *
     * @param fromTopics
     * @param toTopic
     * @param millis
     * @param props
     * @param kStreamFunction
     * @throws InterruptedException
     */
    public void streamToTopic(Collection<String> fromTopics,
                              String toTopic,
                              long millis,
                              Properties props,
                              Function<KStream<K, V>, KStream<K, V>> kStreamFunction
    ) throws InterruptedException {
        /**
         * 创建一个stream，定义一个流，指向主题
         */
        KStream<K, V> kStream = builder.stream(fromTopics);


        kStream = kStreamFunction.apply(kStream);

        /**
         * 把流的数据写入到指定topic
         */
        kStream.to(toTopic);
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


    /**
     * 这个终点在 Printed
     * 使用模板方法 -> 只需要专注于流逻辑的编写
     *
     * @param fromTopics
     * @param printed         终点
     * @param millis
     * @param props
     * @param kStreamFunction
     * @throws InterruptedException
     */
    public void streamToPrint(Collection<String> fromTopics,
                              Printed<K, V> printed,
                              long millis,
                              Properties props,
                              Function<KStream<K, V>, KStream<K, V>> kStreamFunction
    ) throws InterruptedException {
        /**
         * 创建一个stream，定义一个流，指向主题
         */
        KStream<K, V> kStream = builder.stream(fromTopics);


        kStream = kStreamFunction.apply(kStream);

        /**
         * 流的终点
         */
        kStream.print(printed);
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

    /**
     * 这个终点在自定义的 Action
     * 使用模板方法 -> 只需要专注于流逻辑的编写
     *
     * @param fromTopics
     * @param action          终点
     * @param millis
     * @param props
     * @param kStreamFunction
     * @throws InterruptedException
     */
    public void streamToEachAction(Collection<String> fromTopics,
                                   ForeachAction<? super K, ? super V> action,
                                   long millis,
                                   Properties props,
                                   Function<KStream<K, V>, KStream<K, V>> kStreamFunction
    ) throws InterruptedException {
        /**
         * 创建一个stream，定义一个流，指向主题
         */
        KStream<K, V> kStream = builder.stream(fromTopics);


        kStream = kStreamFunction.apply(kStream);

        /**
         * 流的终点
         */
        kStream.foreach(action);
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
