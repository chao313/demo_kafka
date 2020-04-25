package demo.kafka.controller.stream;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;

/**
 * 自行实现的 流处理
 *
 * @param <K>
 * @param <V>
 */
public class MyKStream<K, V> implements KStream<K, V> {
    /**
     * Create a new {@code KStream} that consists of all records of this streamToTopic which satisfy the given predicate.
     * All records that do not satisfy the predicate are dropped.
     * This is a stateless record-by-record operation.
     * <p>
     * 创建一个新的KStream,包含了满足给定断言的所有的records
     * 所有的不满足的records将会被丢弃
     * 这个是一个无状态的逐条记录的操作
     *
     * @see #filterNot(Predicate)
     */
    @Override
    public KStream<K, V> filter(Predicate<? super K, ? super V> predicate) {
        return null;
    }

    /**
     * {@link #filter(Predicate)}
     * 只是多一个拓扑名称
     */
    @Override
    public KStream<K, V> filter(Predicate<? super K, ? super V> predicate, Named named) {
        return null;
    }

    /**
     * {@link #filter(Predicate)}
     * 这个只是NOT而已
     */
    @Override
    public KStream<K, V> filterNot(Predicate<? super K, ? super V> predicate) {
        return null;
    }

    /**
     * {@link #filter(Predicate)}
     * 这个只是NOT而已 + 多拓扑名称
     */
    @Override
    public KStream<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named) {
        return null;
    }

    /**
     * Set a new key (with possibly new type) for each input record.
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new key for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V>}.
     * This is a stateless record-by-record operation.
     * 为每一个输入的record 设置一个新的key(可能是新的类型)
     * 提供的{@link KeyValueMapper}被应用于每一个输入的的record，并且为它计算一个新的key
     * 从而,一个输入record比如  {@code <K,V>} 可以被转化为输出 {@code <K':V>}
     * 这个是逐条的操作
     * <p>
     * For example, you can use this transformation to set a key for a key-less input record {@code <null,V>} by
     * extracting a key from the value within your {@link KeyValueMapper}. The example below computes the new key as the
     * length of the value string.
     * <pre>{@code
     * KStream<Byte[], String> keyLessStream = builder.streamToTopic("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
     * });
     * }</pre>
     * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@code KStream}.
     * 举例，你可以使用这个转换器去为缺失key的输入record {@code <null,V>} 去设置一个key
     * 这个key可以从value中提取(当然也能随机生成)，
     * 举例取计算一个新的key，按照value的长度来计算key
     * <pre>
     * KStream<Byte[], String> keyLessStream = builder.streamToTopic("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
     * });
     * </pre>
     * 设置一个新的key可能会导致内部的重新分配（如果key是聚合或者join）被应用到结果中
     *
     * @param mapper a {@link KeyValueMapper} that computes a new key for each record
     * @param <KR>   the new key type of the result streamToTopic
     * @return a {@code KStream} that contains records with new key (possibly of different type) and unmodified value
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #mapValues(ValueMapperWithKey)
     * @see #flatMapValues(ValueMapper)
     * @see #flatMapValues(ValueMapperWithKey)
     */
    @Override
    public <KR> KStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        return null;
    }

    @Override
    public <KR> KStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper, Named named) {
        return null;
    }

    /**
     * Transform each record of the input streamToTopic into a new record in the output streamToTopic (both key and value type can be
     * altered arbitrarily).
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new output record.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V'>}.
     * This is a stateless record-by-record operation (cf. {@link #transform(TransformerSupplier, String...)} for
     * stateful record transformation).
     * 转换每一个输入的record称为新的输出的stream(key和value可以任意改变)
     * 提供的{@link KeyValueMapper}可以应用于每一条输入的record计算出一个新的output的record
     * 比如: {@code <K,V>} 可以被转换为  {@code <K',V'>}
     * 这个是无状态的逐条的操作(比较有状态的转换 {@link #transform(TransformerSupplier, String...)})
     *
     * <p>
     * The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
     * <pre>{@code
     * KStream<String, String> inputStream = builder.streamToTopic("topic");
     * KStream<String, Integer> outputStream = inputStream.map(new KeyValueMapper<String, String, KeyValue<String, Integer>> {
     *     KeyValue<String, Integer> apply(String key, String value) {
     *         return new KeyValue<>(key.toUpperCase(), value.split(" ").length);
     *     }
     * });
     * }</pre>
     * The provided {@link KeyValueMapper} must return a {@link KeyValue} type and must not return {@code null}.
     * <p>
     * Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@code KStream}. (cf. {@link #mapValues(ValueMapper)})
     *
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @param <KR>   the key type of the result streamToTopic
     * @param <VR>   the value type of the result streamToTopic
     * @return a {@code KStream} that contains records with new key and value (possibly both of different type)
     * @see #selectKey(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #mapValues(ValueMapperWithKey)
     * @see #flatMapValues(ValueMapper)
     * @see #flatMapValues(ValueMapperWithKey)
     * @see #transform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     * @see #transformValues(ValueTransformerWithKeySupplier, String...)
     */
    @Override
    public <KR, VR> KStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return null;
    }

    @Override
    public <KR, VR> KStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named) {
        return null;
    }

    /**
     * 参考 {@link #map(KeyValueMapper)} 这个只是再造value(限制了key的读写)
     */
    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named) {
        return null;
    }

    /**
     * 参考 {@link #map(KeyValueMapper)} 这个只是再造value(限制了key的读(Key是只读的))
     */
    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named) {
        return null;
    }


    /**
     * Transform each record of the input streamToTopic into zero or more records in the output streamToTopic (both key and value type
     * can be altered arbitrarily).
     * The provided {@link KeyValueMapper} is applied to each input record and computes zero or more output records.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K':V'>, <K'':V''>, ...}.
     * This is a stateless record-by-record operation (cf. {@link #transform(TransformerSupplier, String...)} for
     * stateful record transformation).
     * 转换每一条输入的record输出为为0或者更多的record(key和value都能任意更改)
     * 提供的{@link KeyValueMapper}应用于把一条记录变成0或者多条
     * 比如:把{@code <K,V>}变成 {@code <K':V'>, <K'':V''>, ...}
     * <p>
     * The example below splits input records {@code <null:String>} containing sentences as values into their words
     * and emit a record {@code <word:1>} for each word.
     * <pre>{@code
     * KStream<byte[], String> inputStream = builder.streamToTopic("topic");
     * KStream<String, Integer> outputStream = inputStream.flatMap(
     *     new KeyValueMapper<byte[], String, Iterable<KeyValue<String, Integer>>> {
     *         Iterable<KeyValue<String, Integer>> apply(byte[] key, String value) {
     *             String[] tokens = value.split(" ");
     *             List<KeyValue<String, Integer>> result = new ArrayList<>(tokens.length);
     *
     *             for(String token : tokens) {
     *                 result.add(new KeyValue<>(token, 1));
     *             }
     *
     *             return result;
     *         }
     *     });
     * }</pre>
     * The provided {@link KeyValueMapper} must return an {@link Iterable} (e.g., any {@link java.util.Collection} type)
     * and the return value must not be {@code null}.
     * <p>
     * Flat-mapping records might result in an internal data redistribution if a key based operator (like an aggregation
     * or join) is applied to the result {@code KStream}. (cf. {@link #flatMapValues(ValueMapper)})
     *
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @param <KR>   the key type of the result streamToTopic
     * @param <VR>   the value type of the result streamToTopic
     * @return a {@code KStream} that contains more or less records with new key and value (possibly of different type)
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #mapValues(ValueMapperWithKey)
     * @see #flatMapValues(ValueMapper)
     * @see #flatMapValues(ValueMapperWithKey)
     * @see #transform(TransformerSupplier, String...)
     * @see #flatTransform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     * @see #transformValues(ValueTransformerWithKeySupplier, String...)
     * @see #flatTransformValues(ValueTransformerSupplier, String...)
     * @see #flatTransformValues(ValueTransformerWithKeySupplier, String...)
     */
    @Override
    public <KR, VR> KStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper) {
        return null;
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper, Named named) {
        return null;
    }

    /**
     * 参考{@link #flatMap(KeyValueMapper)}，这里限制key是一致的(不可以读写)
     */
    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper, Named named) {
        return null;
    }

    /**
     * 参考{@link #flatMap(KeyValueMapper)}，这里限制key是一致的(可以读不可以写)
     */
    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper, Named named) {
        return null;
    }

    /**
     * Print the records of this KStream using the options provided by {@link Printed}
     * Note that this is mainly for debugging/testing purposes, and it will try to flush on each record print.
     * It <em>SHOULD NOT</em> be used for production usage if performance requirements are concerned.
     * 使用{@link Printed} 打印这个KStream的records
     * 注意主要目的是 debug/test的
     * 不应该被用在prod(如果考虑到性能要求)
     * 1.可以输出到标准输出 {@link Printed#toSysOut()}
     * 2.可以输出到文件 {@link Printed#toFile(String)}
     *
     * @param printed options for printing
     */
    @Override
    public void print(Printed<K, V> printed) {

    }

    /**
     * Perform an action on each record of {@code KStream}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)}).
     * Note that this is a terminal operation that returns void.
     * 为每一条记录执行一个动作
     * 这个是无序的,比较{@link #process(ProcessorSupplier, String...)}
     * 注意这个是终端操作,返回void
     *
     * @param action an action to perform on each record
     * @see #process(ProcessorSupplier, String...)
     */
    @Override
    public void foreach(ForeachAction<? super K, ? super V> action) {

    }

    @Override
    public void foreach(ForeachAction<? super K, ? super V> action, Named named) {

    }

    /**
     * Perform an action on each record of {@code KStream}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)}).
     * <p>
     * Peek is a non-terminal operation that triggers a side effect (such as logging or statistics collection)
     * and returns an unchanged stream.
     * <p>
     * Note that since this operation is stateless, it may execute multiple times for a single record in failure cases.
     * -------------------------------------------------------------------------------------------------------------------
     * 为每一个记录执行一个action
     * 注意是无状态的，比较{@link #process(ProcessorSupplier, String...)}
     * peek是非终端操作,目的是附属作用(日志和统计收集)
     * !!! 返回的是不变的stream
     * 注意是无状态的,失败的情况下,会对一条记录执行多次
     *
     * @param action an action to perform on each record
     * @return itself
     * @see #process(ProcessorSupplier, String...)
     */
    @Override
    public KStream<K, V> peek(ForeachAction<? super K, ? super V> action) {
        return null;
    }

    @Override
    public KStream<K, V> peek(ForeachAction<? super K, ? super V> action, Named named) {
        return null;
    }

    /**
     * Creates an array of {@code KStream} from this stream by branching the records in the original stream based on
     * the supplied predicates.
     * Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
     * Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
     * The branching happens on first-match: A record in the original stream is assigned to the corresponding result
     * stream for the first predicate that evaluates to true, and is assigned to this stream only.
     * A record will be dropped if none of the predicates evaluate to true.
     * This is a stateless record-by-record operation.
     * ---------------------------------------------------------------------------------------------------------------
     * 从当前stream创建一组{@code KStream} ,基于提供的predicates,把原始stream分为多个stream
     * 提供的predicates来评估每条record,会按照顺序来评估
     * 结果中的每个stream和提供的predicates一一对应
     * 分流发生在第一次匹配: 原始流中的一个Record被分配到对应结果的stream中,当第一个predicate被评估为true!!!只会被分配一次
     * !!!如果没有满足要求的，就会被drop
     *
     * @param predicates the ordered list of {@link Predicate} instances
     * @return multiple distinct substreams of this {@code KStream}
     */
    @Override
    public KStream<K, V>[] branch(Predicate<? super K, ? super V>... predicates) {
        return new KStream[0];
    }

    @Override
    public KStream<K, V>[] branch(Named named, Predicate<? super K, ? super V>... predicates) {
        return new KStream[0];
    }

    /**
     * Merge this stream and the given stream into one larger stream.
     * <p>
     * There is no ordering guarantee between records from this {@code KStream} and records from
     * the provided {@code KStream} in the merged stream.
     * Relative order is preserved within each input stream though (ie, records within one input
     * stream are processed in order).
     * ---------------------------------------------------------------------------------------------------------------
     * 合并指定的stream成为更大的stream
     * 1.两个stream之间的合并是无序的
     * 2.每个输入stream是相对有序的
     *
     * @param stream a stream which is to be merged into this stream
     * @return a merged stream containing all records from this and the provided {@code KStream}
     */
    @Override
    public KStream<K, V> merge(KStream<K, V> stream) {
        return null;
    }

    @Override
    public KStream<K, V> merge(KStream<K, V> stream, Named named) {
        return null;
    }

    /**
     * Materialize this stream to a topic and creates a new {@code KStream} from the topic using default serializers,
     * deserializers, and producer's {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is similar to calling {@link #to(String) #to(someTopicName)} and
     * {@link StreamsBuilder#stream(String) StreamsBuilder#stream(someTopicName)}.
     * Note that {@code through()} uses a hard coded {@link org.apache.kafka.streams.processor.FailOnInvalidTimestamp
     * timestamp extractor} and does not allow to customize it, to ensure correct timestamp propagation.
     * ----------------------------------------------------------------------------------------------------------------
     * <p>
     * 使用中转的topic(据说可以重新分区)
     * 实现这个stream到topic 并且创建一个新的Stream使用默认的序列化和反序列化和生产者的默认分区器{@link DefaultPartitioner}.
     * 1.指定的topic应该手动的创建(即在stream启动之前创建)
     * <p>
     * 类似调用to(某个topic) 然后 stream(某个topic)
     *
     * @param topic the topic name
     * @return a {@code KStream} that contains the exact same (and potentially repartitioned) records as this {@code KStream}
     */
    @Override
    public KStream<K, V> through(String topic) {
        return null;
    }

    @Override
    public KStream<K, V> through(String topic, Produced<K, V> produced) {
        return null;
    }

    @Override
    public void to(String topic) {

    }

    @Override
    public void to(String topic, Produced<K, V> produced) {

    }

    /**
     * Dynamically materialize this stream to topics using default serializers specified in the config and producer's
     * {@link DefaultPartitioner}.
     * The topic names for each record to send to is dynamically determined based on the {@link TopicNameExtractor}.
     * 动态的确定stream的to的topic使用默认的序列化和{@link DefaultPartitioner}.
     * topic的name动态的由{@link TopicNameExtractor}指定
     *
     * @param topicExtractor the extractor to determine the name of the Kafka topic to write to for each record
     */
    @Override
    public void to(TopicNameExtractor<K, V> topicExtractor) {

    }


    @Override
    public void to(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced) {

    }

    /**
     * Transform each record of the input stream into zero or one record in the output stream (both key and value type
     * can be altered arbitrarily).
     * ----------------------------------------------------------------------------------------------------
     * 转换输入的每一条记录->0 || 多条 ; key和value可以任意改变
     * A {@link Transformer} (provided by the given {@link TransformerSupplier}) is applied to each input record and
     * returns zero or one output record.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V'>}.
     * ----------------------------------------------------------------------------------------------------
     * 提供的{@link TransformerSupplier}的{@link Transformer}被应用于输入record,返回->0 || 多条
     * 从而 {@code <K,V>}可以转换为 {@code <K':V'>}
     * ----------------------------------------------------------------------------------------------------
     * This is a stateful record-by-record operation (cf. {@link #map(KeyValueMapper) map()}).
     * Furthermore, via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) Punctuator#punctuate()},
     * the processing progress can be observed and additional periodic actions can be performed.
     * ----------------------------------------------------------------------------------------------------
     * 这个是有状态的逐条的操作，比较{@link #map(KeyValueMapper)
     * 此外，通过{@link Punctuator#punctuate(long)} 可以观察到处理进程，并且可以执行额外的定时操作
     * ----------------------------------------------------------------------------------------------------
     * <p>
     * In order to assign a state, the state must be created and registered beforehand (it's not required to connect
     * global state stores; read-only access to global state stores is available by default):
     * ----------------------------------------------------------------------------------------------------
     * 为了分配状态，状态必须预先被创建和注册
     * 不需要连接全局的状态存储,默认全局的state存储是只读的
     * ----------------------------------------------------------------------------------------------------
     * <pre>{@code
     * // create store 创建存储
     * StoreBuilder<KeyValueStore<String,String>> keyValueStoreBuilder =
     *         Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myTransformState"),
     *                 Serdes.String(),
     *                 Serdes.String());
     * // register store 注册存储
     * builder.addStateStore(keyValueStoreBuilder);
     *
     * KStream outputStream = inputStream.transform(new TransformerSupplier() { ... }, "myTransformState");
     * }</pre>
     * Within the {@link Transformer}, the state is obtained via the {@link ProcessorContext}.
     * ----------------------------------------------------------------------------------------------------
     * 在{@link Transformer}中,state通过{@link ProcessorContext}来存储
     * ----------------------------------------------------------------------------------------------------
     * To trigger periodic actions via {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) punctuate()},
     * a schedule must be registered.
     * ----------------------------------------------------------------------------------------------------
     * 注册周期性的action通过{@link Punctuator#punctuate(long) punctuate()}
     * 一个schedule必须被注册
     * ----------------------------------------------------------------------------------------------------
     * The {@link Transformer} must return a {@link KeyValue} type in {@link Transformer#transform(Object, Object)
     * transform()}.
     * The return value of {@link Transformer#transform(Object, Object) Transformer#transform()} may be {@code null},
     * in which case no record is emitted.
     * ----------------------------------------------------------------------------------------------------
     * {@link Transformer} 必须返回一个{@link KeyValue} tyoe 在transform 函数中
     * transform 函数返回的值可能为null(当没有record被发送)
     * ----------------------------------------------------------------------------------------------------
     * <pre>{@code
     * new TransformerSupplier() {
     *     Transformer get() {
     *         return new Transformer() {
     *             private ProcessorContext context;
     *             private StateStore state;
     *
     *             void init(ProcessorContext context) {
     *                 this.context = context;
     *                 this.state = context.getStateStore("myTransformState");
     *                 // punctuate each second; can access this.state
     *                 context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..));
     *             }
     *
     *             KeyValue transform(K key, V value) {
     *                 // can access this.state
     *                 return new KeyValue(key, value); // can emit a single value via return -- can also be null
     *             }
     *
     *             void close() {
     *                 // can access this.state
     *             }
     *         }
     *     }
     * }
     * }</pre>
     * Even if any upstream operation was key-changing, no auto-repartition is triggered.
     * If repartitioning is required, a call to {@link #through(String) through()} should be performed before
     * {@code transform()}.
     * <p>
     * Transforming records might result in an internal data redistribution if a key based operator (like an aggregation
     * or join) is applied to the result {@code KStream}.
     * (cf. {@link #transformValues(ValueTransformerSupplier, String...) transformValues()} )
     * <p>
     * Note that it is possible to emit multiple records for each input record by using
     * {@link ProcessorContext#forward(Object, Object) context#forward()} in
     * {@link Transformer#transform(Object, Object) Transformer#transform()} and
     * {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) Punctuator#punctuate()}.
     * Be aware that a mismatch between the types of the emitted records and the type of the stream would only be
     * detected at runtime.
     * To ensure type-safety at compile-time, {@link ProcessorContext#forward(Object, Object) context#forward()} should
     * not be used in {@link Transformer#transform(Object, Object) Transformer#transform()} and
     * {@link org.apache.kafka.streams.processor.Punctuator#punctuate(long) Punctuator#punctuate()}.
     * If in {@link Transformer#transform(Object, Object) Transformer#transform()} multiple records need to be emitted
     * for each input record, it is recommended to use {@link #flatTransform(TransformerSupplier, String...)
     * flatTransform()}.
     *
     * @param transformerSupplier an instance of {@link TransformerSupplier} that generates a {@link Transformer}
     * @param stateStoreNames     the names of the state stores used by the processor
     * @param <K1>                the key type of the new stream
     * @param <V1>                the value type of the new stream
     * @return a {@code KStream} that contains more or less records with new key and value (possibly of different type)
     * @see #map(KeyValueMapper)
     * @see #flatTransform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     * @see #transformValues(ValueTransformerWithKeySupplier, String...)
     * @see #process(ProcessorSupplier, String...)
     */
    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatTransform(TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <K1, V1> KStream<K1, V1> flatTransform(TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> transformValues(ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> transformValues(ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> transformValues(ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> transformValues(ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatTransformValues(ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier, Named named, String... stateStoreNames) {
        return null;
    }

    @Override
    public void process(ProcessorSupplier<? super K, ? super V> processorSupplier, String... stateStoreNames) {

    }

    @Override
    public void process(ProcessorSupplier<? super K, ? super V> processorSupplier, Named named, String... stateStoreNames) {

    }

    @Override
    public KGroupedStream<K, V> groupByKey() {
        return null;
    }

    @Override
    public KGroupedStream<K, V> groupByKey(Serialized<K, V> serialized) {
        return null;
    }

    @Override
    public KGroupedStream<K, V> groupByKey(Grouped<K, V> grouped) {
        return null;
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> selector) {
        return null;
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> selector, Serialized<KR, V> serialized) {
        return null;
    }

    @Override
    public <KR> KGroupedStream<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> selector, Grouped<KR, V> grouped) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> join(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> join(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, Joined<K, V, VO> joined) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> join(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, StreamJoined<K, V, VO> streamJoined) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, Joined<K, V, VO> joined) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> leftJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, StreamJoined<K, V, VO> streamJoined) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, Joined<K, V, VO> joined) {
        return null;
    }

    @Override
    public <VO, VR> KStream<K, VR> outerJoin(KStream<K, VO> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows, StreamJoined<K, V, VO> streamJoined) {
        return null;
    }

    @Override
    public <VT, VR> KStream<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        return null;
    }

    @Override
    public <VT, VR> KStream<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined) {
        return null;
    }

    @Override
    public <VT, VR> KStream<K, VR> leftJoin(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        return null;
    }

    @Override
    public <VT, VR> KStream<K, VR> leftJoin(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined) {
        return null;
    }

    @Override
    public <GK, GV, RV> KStream<K, RV> join(GlobalKTable<GK, GV> globalKTable, KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper, ValueJoiner<? super V, ? super GV, ? extends RV> joiner) {
        return null;
    }

    @Override
    public <GK, GV, RV> KStream<K, RV> join(GlobalKTable<GK, GV> globalKTable, KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper, ValueJoiner<? super V, ? super GV, ? extends RV> joiner, Named named) {
        return null;
    }

    @Override
    public <GK, GV, RV> KStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalKTable, KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper, ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner) {
        return null;
    }

    @Override
    public <GK, GV, RV> KStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalKTable, KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper, ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner, Named named) {
        return null;
    }
}
