package demo.kafka.controller.stream.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.stream.KStreamModel;
import demo.kafka.controller.stream.PropertiesStreamFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class KStreamModelTest extends KStreamBase {

    /**
     * 流构造器
     */
    protected static StreamsBuilder builder = new StreamsBuilder();
    ;

    /**
     * 测试再造Key
     *
     * @throws InterruptedException
     */
    @Test
    public void selectKeyTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToTopic(
                Arrays.asList("Test"),
                "output",
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        KStream<String, String> resultKStream = kStream.selectKey(new KeyValueMapper<String, String, String>() {
                            @Override
                            public String apply(String key, String value) {
                                return "11";
                            }
                        });
                        return resultKStream;
                    }
                });
    }

    /**
     * 测试再造Key,value
     *
     * @throws InterruptedException
     */
    @Test
    public void mapTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToTopic(
                Arrays.asList("Test"),
                "output",
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        KStream<String, String> resultKStream = kStream.map(new KeyValueMapper<String, String, KeyValue<? extends String, ? extends String>>() {
                            @Override
                            public KeyValue<? extends String, ? extends String> apply(String key, String value) {
                                String format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(new Date());
                                return new KeyValue<>(key + ":" + format, value + ":" + format);
                            }
                        });
                        return resultKStream;
                    }
                });
    }


    /**
     * 测试再造value
     *
     * @throws InterruptedException
     */
    @Test
    public void mapValuesValueMapperTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToTopic(
                Arrays.asList("Test"),
                "output",
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        KStream<String, String> resultKStream = kStream.mapValues(new ValueMapper<String, String>() {
                            @Override
                            public String apply(String value) {
                                return "使用mapValues的ValueMapper来操作value:" + value;
                            }
                        });
                        return resultKStream;
                    }
                });
    }

    /**
     * 测试再造value
     *
     * @throws InterruptedException
     */
    @Test
    public void mapValuesValueMapperWithKeyTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToTopic(
                Arrays.asList("Test"),
                "output",
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        KStream<String, String> resultKStream = kStream.mapValues(new ValueMapperWithKey<String, String, String>() {

                            @Override
                            public String apply(String readOnlyKey, String value) {
                                return "使用mapValues的 ValueMapperWithKey 来操作value(readOnlyKey):" + value;
                            }
                        });
                        return resultKStream;
                    }
                });
    }

    /**
     * 测试1->0 || 1->more
     *
     * @throws InterruptedException
     */
    @Test
    public void flatMapTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToTopic(
                Arrays.asList("Test"),
                "output",
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        KStream<String, String> resultKStream = kStream.flatMap(new KeyValueMapper<String, String, Iterable<? extends KeyValue<? extends String, ? extends String>>>() {
                            @Override
                            public Iterable<? extends KeyValue<? extends String, ? extends String>> apply(String key, String value) {
                                List<KeyValue<String, String>> result = new ArrayList<>();
                                for (char c : value.toCharArray()) {
                                    result.add(new KeyValue<>(key, String.valueOf(c)));
                                }
                                return result;
                            }
                        });
                        return resultKStream;
                    }
                });
    }

    /**
     * 测试print到标准数据 测试可以
     * [KSTREAM-SOURCE-0000000000]: key, value
     * [KSTREAM-SOURCE-0000000000]: key, value
     * ......
     *
     * @throws InterruptedException
     */
    @Test
    public void printToSysOutTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToPrint(
                Arrays.asList("Test"),
                Printed.toSysOut(),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        return kStream;
                    }
                });
    }

    /**
     * 测试print到file 测试可以
     * ！！！ 似乎会覆盖
     * [KSTREAM-SOURCE-0000000000]: key, value
     * [KSTREAM-SOURCE-0000000000]: key, value
     * ......
     *
     * @throws InterruptedException
     */
    @Test
    public void printToFileTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamToPrint(
                Arrays.asList("Test"),
                Printed.toFile("topic"),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()), new Function<KStream<String, String>, KStream<String, String>>() {
                    @Override
                    public KStream<String, String> apply(KStream<String, String> kStream) {
                        return kStream;
                    }
                });
    }


    /**
     * 测试分流 -> success
     *
     * @throws InterruptedException
     */
    @Test
    public void branchTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamNoTo(
                Arrays.asList("Test"),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()),
                new Consumer<KStream<String, String>>() {
                    @Override
                    public void accept(KStream<String, String> kStream) {
                        KStream<String, String>[] branch = kStream.branch(new Predicate<String, String>() {
                            @Override
                            public boolean test(String key, String value) {
                                return key.equalsIgnoreCase("key111");
                            }
                        }, new Predicate<String, String>() {
                            @Override
                            public boolean test(String key, String value) {
                                return key.equalsIgnoreCase("key");
                            }
                        });
                        branch[0].to("key111");
                        branch[1].to("key");
                    }
                }
        );
    }

    /**
     * 测试 合流 merge -> success
     *
     * @throws InterruptedException
     */
    @Test
    public void mergeTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamNoTo(
                Arrays.asList("Test"),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()),
                new Consumer<KStream<String, String>>() {
                    @Override
                    public void accept(KStream<String, String> kStream) {
                        KStream<String, String>[] branch = kStream.branch(new Predicate<String, String>() {
                            @Override
                            public boolean test(String key, String value) {
                                return key.equalsIgnoreCase("key222");
                            }
                        }, new Predicate<String, String>() {
                            @Override
                            public boolean test(String key, String value) {
                                return key.equalsIgnoreCase("key111");
                            }
                        }, new Predicate<String, String>() {
                            @Override
                            public boolean test(String key, String value) {
                                return key.equalsIgnoreCase("key");
                            }
                        });
//                        branch[0].to("key222");
//                        branch[1].to("key111");
//                        branch[2].to("key");

                        branch[0].merge(branch[1]).to("111222");
                    }
                }
        );
    }

    /**
     * 测试  through 这个是中转的效果(必须要手动创建)
     */
    @Test
    public void throughTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamNoTo(
                Arrays.asList("Test"),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()),
                new Consumer<KStream<String, String>>() {
                    @Override
                    public void accept(KStream<String, String> kStream) {
                        kStream.through("111222");
                    }
                }
        );
    }

    /**
     * 测试 动态的发送到topic
     * !! 时候时间戳无法改变
     */
    @Test
    public void TopicNameExtractorTest() throws InterruptedException {

        KStreamModel<String, String> kStreamModel = new KStreamModel();
        kStreamModel.streamNoTo(
                Arrays.asList("Test"),
                5000L,
                PropertiesStreamFactory.create("id", Bootstrap.HONE.getIp()),
                new Consumer<KStream<String, String>>() {
                    @Override
                    public void accept(KStream<String, String> kStream) {
                        kStream.to(new TopicNameExtractor<String, String>() {


                            /**
                             * Extracts the topic name to send to. The topic name must already exist, since the Kafka Streams library will not
                             * try to automatically create the topic with the extracted name.
                             * 提取topic的Name去发送,topic必须存在(因为stream不会自动的创建topic)
                             * @param key           the record key
                             * @param value         the record value
                             * @param recordContext current context metadata of the record  当前record的元数据
                             * @return the topic name this record should be sent to 记录应该发送的topic
                             */
                            @Override
                            public String extract(String key, String value, RecordContext recordContext) {
                                return key;
                            }
                        });
                    }
                }
        );
    }

//    /**
//     * 测试 动态的发送到topic
//     * !! 时候时间戳无法改变
//     */
//    @Test
//    public void transformTest() throws InterruptedException {
//        String key = "myTransformState";
//
//        StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
//                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(key),
//                        Serdes.String(),
//                        Serdes.String());
//        // register store 注册存储
//        builder.addStateStore(keyValueStoreBuilder);
//        KStream<Object, Object> kStream = builder.stream("Test");
//
//        KStream<String,String> outputStream = kStream.transform(new TransformerSupplier<String, String, String>() {
//
//
//            @Override
//            public Transformer<String, String, String> get() {
//                return null;
//            }
//        }, "myTransformState");
//    }


}
