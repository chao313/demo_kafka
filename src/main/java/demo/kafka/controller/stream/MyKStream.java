package demo.kafka.controller.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
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
     * Create a new {@code KStream} that consists of all records of this stream which satisfy the given predicate.
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
     *
     * <p>
     * For example, you can use this transformation to set a key for a key-less input record {@code <null,V>} by
     * extracting a key from the value within your {@link KeyValueMapper}. The example below computes the new key as the
     * length of the value string.
     * <pre>{@code
     * KStream<Byte[], String> keyLessStream = builder.stream("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
     * });
     * }</pre>
     * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@code KStream}.
     *
     * @param mapper a {@link KeyValueMapper} that computes a new key for each record
     * @param <KR>   the new key type of the result stream
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

    @Override
    public <KR, VR> KStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return null;
    }

    @Override
    public <KR, VR> KStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named) {
        return null;
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper) {
        return null;
    }

    @Override
    public <KR, VR> KStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper, Named named) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper, Named named) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return null;
    }

    @Override
    public <VR> KStream<K, VR> flatMapValues(ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper, Named named) {
        return null;
    }

    @Override
    public void print(Printed<K, V> printed) {

    }

    @Override
    public void foreach(ForeachAction<? super K, ? super V> action) {

    }

    @Override
    public void foreach(ForeachAction<? super K, ? super V> action, Named named) {

    }

    @Override
    public KStream<K, V> peek(ForeachAction<? super K, ? super V> action) {
        return null;
    }

    @Override
    public KStream<K, V> peek(ForeachAction<? super K, ? super V> action, Named named) {
        return null;
    }

    @Override
    public KStream<K, V>[] branch(Predicate<? super K, ? super V>... predicates) {
        return new KStream[0];
    }

    @Override
    public KStream<K, V>[] branch(Named named, Predicate<? super K, ? super V>... predicates) {
        return new KStream[0];
    }

    @Override
    public KStream<K, V> merge(KStream<K, V> stream) {
        return null;
    }

    @Override
    public KStream<K, V> merge(KStream<K, V> stream, Named named) {
        return null;
    }

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

    @Override
    public void to(TopicNameExtractor<K, V> topicExtractor) {

    }

    @Override
    public void to(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced) {

    }

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
