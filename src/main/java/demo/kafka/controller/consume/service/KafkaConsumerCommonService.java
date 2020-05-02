package demo.kafka.controller.consume.service;

import demo.kafka.controller.response.EChartsVo;
import demo.kafka.controller.response.LineEChartsVo;
import demo.kafka.util.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import scala.collection.immutable.HashMapBuilder;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 增强版(使用组合模式)
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class KafkaConsumerCommonService<K, V> {

    public static final String __consumer_offsets = "__consumer_offsets";


    /**
     * 获取指定的offset record (每个分区的)
     */
    public ConsumerRecord<K, V> getOneRecord(String bootstrap_servers, TopicPartition topicPartition, long offset) {

        /**
         * 获取一个消费者实例
         */

        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        KafkaConsumer<K, V> instance
                = consumerFactory.getKafkaConsumer();
        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));

        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);

        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        if (records.records(topicPartition).size() > 0) {
            return records.records(topicPartition).get(0);
        } else {
            return null;
        }
    }

    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<K, V>> getRecord(String bootstrap_servers,
                                                TopicPartition topicPartition,
                                                long offset,
                                                int recordsNum) {

        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(recordsNum))
        );
        KafkaConsumer<K, V> instance = consumerFactory.getKafkaConsumer();

        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));

        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);

        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        return records.records(topicPartition);
    }

    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<String, String>> getRecord(String bootstrap_servers,
                                                          TopicPartition topicPartition,
                                                          long startOffset,
                                                          long endOffset,
                                                          String keyRegex,
                                                          String valueRegex,
                                                          Long timeStart,
                                                          Long timeEnd

    ) {
        /** 记录原始的数据*/
        Long timeStartOriginal = null;
        if (null != timeStart) {
            timeStartOriginal = new Long(timeStart);
        }
        Long timeEndOriginal = null;
        if (null != timeEnd) {
            timeEndOriginal = new Long(timeEnd);
        }

        /**
         * 获取一个消费者实例 (设置一次性读取出全部的record)
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(endOffset - startOffset))
        );
        /**
         * 根据时间来限制范围
         */
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        if (null != timeStart) {
            OffsetAndTimestamp firstPartitionOffsetAfterStartTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeStart);
            if (null != firstPartitionOffsetAfterStartTimestamp) {
                if (firstPartitionOffsetAfterStartTimestamp.offset() > startOffset) {
                    startOffset = firstPartitionOffsetAfterStartTimestamp.offset();//取范围小的
                }
            }
        }
        if (null != timeEnd) {
            /**根据指定的 结束时间获取偏移量 */
            OffsetAndTimestamp firstPartitionOffsetAfterEndTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeEnd);
            if (null != firstPartitionOffsetAfterEndTimestamp) {
                if (firstPartitionOffsetAfterEndTimestamp.offset() < endOffset) {
                    endOffset = firstPartitionOffsetAfterEndTimestamp.offset();//取范围小的
                }
            }
        }
        KafkaConsumer<String, String> instance = consumerFactory.getKafkaConsumer();
        /**分配 topicPartition*/
        instance.assign(Arrays.asList(topicPartition));
        /**设置偏移量*/
        instance.seek(topicPartition, startOffset);
        /**获取记录*/
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        /**获取记录*/
        ConsumerRecords<String, String> records;
        boolean flag = true;
        do {
            records = instance.poll(1000);
            log.info("再次poll:records.count():{}", records.count());

            for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                boolean keyRegexFlag = false,
                        valueRegexFlag = false;
                if (StringUtils.isBlank(keyRegex)) {
                    keyRegexFlag = true;
                } else {
                    String key = record.key();
                    keyRegexFlag = key.matches(keyRegex);
                }
                if (StringUtils.isBlank(valueRegex)) {
                    valueRegexFlag = true;
                } else {
                    String value = record.value();
                    valueRegexFlag = value.matches(valueRegex);
                }
                if (record.offset() > endOffset) {
                    /**
                     * 如果超出范围就截止
                     */
                    log.info("截止:");
                    flag = false;
                    break;
                }
                if (keyRegexFlag && valueRegexFlag && record.offset() <= endOffset) {
                    boolean timeStartFlag = false,
                            timeEndFlag = false;
                    /**
                     * 全部符合要求
                     * 加上时间判断 为空 或者 < timeEnd >  timeStart
                     */
                    if (null == timeEndOriginal || (null != timeEndOriginal && record.timestamp() <= timeEndOriginal)) {
                        timeEndFlag = true;
                    }
                    if (null == timeStartOriginal || (null != timeStartOriginal && record.timestamp() >= timeStartOriginal)) {
                        timeStartFlag = true;
                    }
                    if (timeStartFlag && timeEndFlag) {
                        result.add(record);
                    }
                }
            }
        } while (records.count() != 0 && flag == true);
        instance.close();
        return result;
    }

    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<String, String>> getRecord(String bootstrap_servers,
                                                          TopicPartition topicPartition,
                                                          String keyRegex,
                                                          String valueRegex,
                                                          Long timeStart,
                                                          Long timeEnd

    ) {
        /** 记录原始的数据*/
        Long timeStartOriginal = null;
        if (null != timeStart) {
            timeStartOriginal = new Long(timeStart);
        }
        Long timeEndOriginal = null;
        if (null != timeEnd) {
            timeEndOriginal = new Long(timeEnd);
        }
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        /**
         * 获取一个消费者实例 (设置一次性读取出全部的record)
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$()
        );
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        /**获取Partition最早的时间*/
        OffsetAndTimestamp earliestRecordOffsetAndTimestamp = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, 0L);
        /**获取Partition最新的时间*/
        OffsetAndTimestamp latestRecordOffsetAndTimestamp = consumerNoGroupService.getLastPartitionOffsetAndTimestamp(topicPartition);

        if (null == earliestRecordOffsetAndTimestamp) {
            /**开始为null 无需继续*/
            return result;
        }
        /**
         * 根据时间来限制范围
         */
        if (null != timeStart) {
            /**如果开始不为null*/
            OffsetAndTimestamp firstPartitionOffsetAfterStartTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeStart);
            /**比较取范围小的*/
            if (null != firstPartitionOffsetAfterStartTimestamp && firstPartitionOffsetAfterStartTimestamp.timestamp() > earliestRecordOffsetAndTimestamp.timestamp()) {
                timeStart = firstPartitionOffsetAfterStartTimestamp.timestamp();
            } else {
                timeStart = earliestRecordOffsetAndTimestamp.timestamp();
            }
        } else {
            /**为null , 取最开始*/
            timeStart = earliestRecordOffsetAndTimestamp.timestamp();
        }
        if (null != timeEnd) {
            OffsetAndTimestamp firstPartitionOffsetAfterEndTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeEnd);
            /**比较取范围小的*/
            if (null != firstPartitionOffsetAfterEndTimestamp && firstPartitionOffsetAfterEndTimestamp.timestamp() < latestRecordOffsetAndTimestamp.timestamp()) {
                timeEnd = firstPartitionOffsetAfterEndTimestamp.timestamp();
            } else {
                timeEnd = latestRecordOffsetAndTimestamp.timestamp();
            }
        } else {
            /**为null , 取最新*/
            timeEnd = latestRecordOffsetAndTimestamp.timestamp();
        }

        /**!!!准备好开始和结尾 ， 开始计算*/
        OffsetAndTimestamp startOffset = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeStart);
        OffsetAndTimestamp endOffset = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeEnd);


        KafkaConsumer<String, String> instance = consumerFactory.getKafkaConsumer();
        /**分配 topicPartition*/
        instance.assign(Arrays.asList(topicPartition));
        /**设置偏移量*/
        instance.seek(topicPartition, startOffset.offset());
        /**获取记录*/
        ConsumerRecords<String, String> records;
        boolean flag = true;
        do {
            records = instance.poll(1000);
            log.info("再次poll:records.count():{}", records.count());

            for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                boolean keyRegexFlag = false,
                        valueRegexFlag = false;
                if (StringUtils.isBlank(keyRegex)) {
                    keyRegexFlag = true;
                } else {
                    String key = record.key();
                    keyRegexFlag = key.matches(keyRegex);
                }
                if (StringUtils.isBlank(valueRegex)) {
                    valueRegexFlag = true;
                } else {
                    String value = record.value();
                    valueRegexFlag = value.matches(valueRegex);
                }
                if (record.offset() > endOffset.offset()) {
                    /**
                     * 如果超出范围就截止
                     */
                    log.info("截止:");
                    flag = false;
                    break;
                }
                if (keyRegexFlag && valueRegexFlag && record.offset() <= endOffset.offset()) {
                    boolean timeStartFlag = false,
                            timeEndFlag = false;
                    /**
                     * 全部符合要求
                     * 加上时间判断 为空 或者 < timeEnd >  timeStart
                     */
                    if (null == timeEndOriginal || (null != timeEndOriginal && record.timestamp() <= timeEndOriginal)) {
                        timeEndFlag = true;
                    }
                    if (null == timeStartOriginal || (null != timeStartOriginal && record.timestamp() >= timeStartOriginal)) {
                        timeStartFlag = true;
                    }
                    if (timeStartFlag && timeEndFlag) {
                        result.add(record);
                    }
                }
            }
        } while (records.count() != 0 && flag == true);
        instance.close();
        return result;
    }

    /**
     * 获取 指定查询条件的 Echarts
     * {
     * title: {
     * text: '消息msg图'
     * },
     * tooltip: {},
     * xAxis: {
     * data: ['衬衫', '羊毛衫', '雪纺衫', '裤子', '高跟鞋', '袜子']
     * },
     * yAxis: {},
     * series: [{
     * name: '消息msg图',
     * type: 'bar',
     * data: [5, 20, 36, 10, 10, 20]
     * }]
     * }
     */
    public enum Level {
        YEAR("yyyy"),
        MONTH("yyyy-MM"),
        DAY("yyyy-MM-dd"),
        HOUR("yyyy-MM-dd HH"),
        MINUTES("yyyy-MM-dd HH:mm"),
        SECONDS("yyyy-MM-dd HH:mm:ss"),
        MILLISECOND("yyyy-MM-dd HH:mm:ss.S");
        private String format;

        Level(String format) {
            this.format = format;
        }
    }

    public EChartsVo getRecordECharts(String bootstrap_servers,
                                      TopicPartition topicPartition,
                                      long startOffset,
                                      long endOffset,
                                      String keyRegex,
                                      String valueRegex,
                                      Long timeStart,
                                      Long timeEnd,
                                      Level level //画图的级别 {"yyyy-MM-dd HH:mm:ss..."}

    ) {


        Map<String, Long> resultMap = new HashMap<>();
        FastDateFormat fastDateFormat = FastDateFormat.getInstance(level.format);


        /**
         * 获取一个消费者实例 (设置一次性读取出全部的record)
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(endOffset - startOffset))
        );
        /**
         * 根据时间来限制范围
         */
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        if (null != timeStart) {
            OffsetAndTimestamp firstPartitionOffsetAfterStartTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeStart);
            if (null != firstPartitionOffsetAfterStartTimestamp) {
                if (firstPartitionOffsetAfterStartTimestamp.offset() > startOffset) {
                    startOffset = firstPartitionOffsetAfterStartTimestamp.offset();//取范围小的
                }
            }
        }
        if (null != timeEnd) {
            OffsetAndTimestamp firstPartitionOffsetAfterEndTimestamp
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, timeEnd);
            if (null != firstPartitionOffsetAfterEndTimestamp) {
                if (firstPartitionOffsetAfterEndTimestamp.offset() < endOffset) {
                    endOffset = firstPartitionOffsetAfterEndTimestamp.offset();//取范围小的
                }
            }
        }


        KafkaConsumer<String, String> instance = consumerFactory.getKafkaConsumer();
        /**分配 topicPartition*/
        instance.assign(Arrays.asList(topicPartition));
        /**设置偏移量*/
        instance.seek(topicPartition, startOffset);
        /**获取记录*/
        ConsumerRecords<String, String> records;
        boolean flag = true;
        do {
            records = instance.poll(1000);
            log.info("再次poll:records.count():{}", records.count());
            for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                boolean keyRegexFlag = false,
                        valueRegexFlag = false;
                if (StringUtils.isBlank(keyRegex)) {
                    keyRegexFlag = true;
                } else {
                    if (StringUtils.isBlank(record.key())) {
                        keyRegexFlag = false;
                    } else {
                        String key = record.key();
                        keyRegexFlag = key.matches(keyRegex);
                    }
                }
                if (StringUtils.isBlank(valueRegex)) {
                    valueRegexFlag = true;
                } else {
                    if (StringUtils.isBlank(record.value())) {
                        valueRegexFlag = false;
                    } else {
                        String value = record.value();
                        valueRegexFlag = value.matches(valueRegex);
                    }
                }
                if (record.offset() > endOffset) {
                    /**
                     * 如果超出范围就截止
                     */
                    log.info("截止:");
                    flag = false;
                    break;
                }
                if (keyRegexFlag && valueRegexFlag && record.offset() < endOffset) {
                    /**
                     * 全部符合要求 日期也符合要求
                     */
                    String dateStr = fastDateFormat.format(record.timestamp());
                    if (resultMap.containsKey(dateStr)) {
                        /**
                         * 如果存在就 +1
                         */
                        Long sum = resultMap.get(dateStr) + 1;
                        resultMap.put(dateStr, sum);
                    } else {
                        /**
                         * 如果不存在就 赋值 0
                         */
                        resultMap.put(dateStr, new Long(1));
                    }
                }
            }
        } while (records.count() != 0 && flag == true);
        /**
         * 排序
         */
        resultMap = this.sortHashMap(resultMap);

        EChartsVo builder = EChartsVo.builder("bar");

        builder.addXAxisData(resultMap.keySet());//添加x轴数据

        builder.addSeriesData(resultMap.values());//添加x轴数据

        instance.close();
        return builder.end();
    }


    public enum LevelSimple {
        YEAR("yyyy", "yyyy", Calendar.YEAR),
        MONTH("yyyyMM", "yyyy-MM", Calendar.MONTH),
        DAY("yyyyMMdd", "yyyy-MM-dd", Calendar.DAY_OF_MONTH),
        HOUR("yyyyMMddHH", "yyyy-MM-dd HH", Calendar.HOUR_OF_DAY),
        MINUTES("yyyyMMddHHmm", "yyyy-MM-dd HH:mm", Calendar.MINUTE),
        SECONDS("yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", Calendar.SECOND),
        MILLISECOND("yyyyMMddHHmmssS", "yyyy-MM-dd HH:mm:ss.S", Calendar.MILLISECOND);
        private String format;
        private String toFormat;
        private Integer field;

        LevelSimple(String format, String toFormat, Integer field) {
            this.format = format;
            this.toFormat = toFormat;
            this.field = field;
        }
    }

    /**
     * 获取简单级别的画图(Partition级别的)
     * 这里以时间来作为区分点！！！
     */
    public EChartsVo getRecordTopicPartitionSimpleECharts(String bootstrap_servers,
                                                          TopicPartition topicPartition,
                                                          Long timeStart,
                                                          Long timeEnd,
                                                          LevelSimple levelSimple //画图的级别

    ) throws ParseException {
        Map<String, Long> resultMap = this.getRecordTopicPartitionSimpleMap(bootstrap_servers, topicPartition, timeStart, timeEnd, levelSimple);
        /**
         * 排序 移除0的数据
         */
        Map<String, Long> result = new HashMap<>();
        resultMap.forEach((key, value) -> {
            if (value != 0) {
                result.put(key, value);
            }
        });
        resultMap = this.sortHashMap(result);

        EChartsVo builder = EChartsVo.builder("bar");

        builder.addXAxisData(resultMap.keySet());//添加x轴数据

        builder.addSeriesData(resultMap.values());//添加x轴数据

        return builder.end();
    }

    /**
     * 获取简单级别的画图(Topic级别的)
     * 这里以时间来作为区分点！！！
     */
    public EChartsVo getRecordTopicSimpleECharts(String bootstrap_servers,
                                                 Collection<TopicPartition> topicPartitions,
                                                 Long timeStart,
                                                 Long timeEnd,
                                                 LevelSimple levelSimple //画图的级别

    ) throws ParseException {
        Map<String, Long> resultMap = new ConcurrentHashMap<>();
        /**遍历所有的结果，相同就相加，不同就添加 **/
        /**现在改为并发执行*/
        Map<String, Long> finalResultMap = resultMap;
        topicPartitions.parallelStream().forEach(topicPartition -> {
            Map<String, Long> resultTmp = null;
            try {
                resultTmp = this.getRecordTopicPartitionSimpleMap(bootstrap_servers, topicPartition, timeStart, timeEnd, levelSimple);
                for (Map.Entry<String, Long> entry : resultTmp.entrySet()) {
                    String key = entry.getKey();
                    Long value = entry.getValue();
                    if (finalResultMap.containsKey(key)) {
                        Long old = finalResultMap.get(key);
                        finalResultMap.put(key, old + value);
                    } else {
                        finalResultMap.put(key, value);
                    }
                }
            } catch (ParseException e) {
                log.info("e:{}", e.toString(), e);
            }
        });

        /**
         * 排序 移除0的数据
         */
        Map<String, Long> result = new HashMap<>();
        resultMap.forEach((key, value) -> {
            if (value != 0) {
                result.put(key, value);
            }
        });
        resultMap = this.sortHashMap(result);
        EChartsVo builder = EChartsVo.builder("bar");
        builder.addXAxisData(resultMap.keySet());//添加x轴数据
        builder.addSeriesData(resultMap.values());//添加x轴数据
        return builder.end();
    }

    /**
     * 获取简单级别的画图(折线图)
     * 这里以时间来作为区分点！！！
     */
    public LineEChartsVo getRecordLineECharts(String bootstrap_servers,
                                              TopicPartition topicPartition,
                                              Long timeStart,
                                              Long timeEnd,
                                              LevelSimple levelSimple //画图的级别

    ) throws ParseException {
        Map<String, Long> resultMap = this.getRecordLineEChartsMap(bootstrap_servers, topicPartition, timeStart, timeEnd, levelSimple, null);
        /**
         * 排序
         */
        resultMap = this.sortHashMap(resultMap);
        LineEChartsVo builder = LineEChartsVo.builder();
        builder.addXAxisData(resultMap.keySet());//添加x轴数据
        builder.addSeriesData(resultMap.values());//添加x轴数据
        return builder.end();
    }

    /**
     * 获取简单级别的画图(折线图) 这个是集合
     * 这里以时间来作为区分点！！！
     */
    public LineEChartsVo getRecordLineECharts(String bootstrap_servers,
                                              Collection<TopicPartition> topicPartitions,
                                              Long timeStart,
                                              Long timeEnd,
                                              LevelSimple levelSimple, //画图的级别,
                                              Set<Long> timeStamps//需要插入的点

    ) throws ParseException {
        Map<String, Long> resultMap = new HashMap<>();
        Map<String, Long> finalResultMap = resultMap;


        for (TopicPartition topicPartition : topicPartitions) {
            try {
                Map<String, Long> resultTmp
                        = this.getRecordLineEChartsMap(bootstrap_servers, topicPartition, timeStart, timeEnd, levelSimple, timeStamps);
                if (null != resultTmp) {
                    for (Map.Entry<String, Long> entry : resultTmp.entrySet()) {
                        String key = entry.getKey();
                        Long value = entry.getValue();
                        if (finalResultMap.containsKey(key)) {
                            Long old = finalResultMap.get(key);
                            finalResultMap.put(key, old + value);
                        } else {
                            finalResultMap.put(key, value);
                        }
                    }
                }
            } catch (ParseException e) {
                log.info("e:{}", e.toString(), e);
            }
        }
        /**
         * 排序
         */
        resultMap = this.sortHashMap(finalResultMap);
        LineEChartsVo builder = LineEChartsVo.builder();
        builder.addXAxisData(resultMap.keySet());//添加x轴数据
        builder.addSeriesData(resultMap.values());//添加x轴数据
        return builder.end();
    }


    /**
     * 获取 指定offset的 指定数量的 record
     */
    public List<ConsumerRecord<K, V>> getRecord(String bootstrap_servers, TopicPartition topicPartition, long offset, int recordsNum, Map overMap) {

        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<K, V> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers,
                MapUtil.$(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(recordsNum))
        );
        KafkaConsumer<K, V> instance = consumerFactory.getKafkaConsumer();
        /**
         * 分配 topicPartition
         */
        instance.assign(Arrays.asList(topicPartition));
        /**
         * 设置偏移量
         */
        instance.seek(topicPartition, offset);
        /**
         * 获取一条记录
         */
        ConsumerRecords<K, V> records = instance.poll(1000);

        instance.close();
        return records.records(topicPartition);
    }

    private Map<String, Long> sortHashMap(Map<String, Long> map) {
        //從HashMap中恢復entry集合，得到全部的鍵值對集合
        Set<Map.Entry<String, Long>> entey = map.entrySet();
        //將Set集合轉為List集合，為了實用工具類的排序方法
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(entey);
        //使用Collections工具類對list進行排序
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                //按照age倒敘排列
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        //創建一個HashMap的子類LinkedHashMap集合
        LinkedHashMap<String, Long> linkedHashMap = new LinkedHashMap<>();
        //將list中的數據存入LinkedHashMap中
        for (Map.Entry<String, Long> entry : list) {
            linkedHashMap.put(entry.getKey(), entry.getValue());
        }
        return linkedHashMap;
    }

    /**
     * 获取简单级别的画图
     * 这里以时间来作为区分点！！！
     */
    public Map<String, Long> getRecordTopicPartitionSimpleMap(String bootstrap_servers,
                                                              TopicPartition topicPartition,
                                                              Long timeStart,
                                                              Long timeEnd,
                                                              LevelSimple levelSimple //画图的级别

    ) throws ParseException {
        /**
         * 记录 beforeNode 和  afterNode 的差值， 并附在 before 的可以上
         */
        Map<String, Long> resultMap = new HashMap<>();

        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.toFormat);
        if (null != timeStart && null != timeEnd && timeStart > timeEnd) {
            throw new RuntimeException("选择的start日期不能大于end日期:"
                    + "start:" + fastDateToFormat.format(new Date(timeStart))
                    + "end  :" + fastDateToFormat.format(new Date(timeEnd))
            );
        }
        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        /**
         * 根据时间来限制范围
         */
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();

        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService
                = consumerFactory.getConsumerHavGroupAssignService(topicPartition);

        /**获取最早的时间*/
        OffsetAndTimestamp earliestRecordOffsetAndTimestamp = consumerHavGroupAssignService.getFirstPartitionOffsetAfterTimestamp(topicPartition, 0L);
        /**获取最晚的时间*/
        OffsetAndTimestamp latestRecordOffsetAndTimestamp = consumerHavGroupAssignService.getLastPartitionOffsetAndTimestamp(topicPartition);

        if (earliestRecordOffsetAndTimestamp == null) {
            return resultMap;
        }

        if (null != timeStart && earliestRecordOffsetAndTimestamp.timestamp() > timeStart) {
            /**
             * 选择出范围小的时间
             */
            timeStart = earliestRecordOffsetAndTimestamp.timestamp();
        } else if (null == timeStart) {
            timeStart = earliestRecordOffsetAndTimestamp.timestamp();
        }

        if (null != timeEnd && latestRecordOffsetAndTimestamp.timestamp() < timeEnd) {
            /**
             * 选择出范围小的时间
             */
            timeEnd = latestRecordOffsetAndTimestamp.timestamp();
        } else if (null == timeEnd) {
            timeEnd = latestRecordOffsetAndTimestamp.timestamp();
        }

        /**
         * !!!!开始和结束的区间！！！
         */
        Long beginOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeStart);
        Long endOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeEnd) + 1;
        //

        //这里已经算好了 开始和结束的时机 下面就是累计相加

        Date timeStartDate = new Date(timeStart);
        Date timeEndDate = new Date(timeEnd);


        Date firstNode = DateUtils.ceiling(timeStartDate, levelSimple.field);//有效的第一个节点
        Date endNode = DateUtils.truncate(timeEndDate, levelSimple.field);//有效的最后一个节点

        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(firstNode);
        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(endNode);


        if (endNode.after(firstNode)) {
            /**
             * 如果最后的节点 > 最新的节点(正常)
             */
            /**
             * 补全第一个的数据
             */
            String keyFirst = fastDateToFormat.format(DateUtils.truncate(timeStartDate, levelSimple.field));//key取第一个节点
            Long afterFirstNode = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, calendarStart.getTime().getTime());
            resultMap.put(keyFirst, afterFirstNode - beginOffset);

            /**
             * 补全中间的节点
             */
            while (calendarStart.before(calendarEnd)) {
                String key = fastDateToFormat.format(calendarStart.getTime());
                Long beforeNode = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, calendarStart.getTime().getTime());
                /** 遍历 移动 直到和calendarEnd相同 */
                calendarStart.add(levelSimple.field, 1);//移到到下一个节点
                Long afterNode = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, calendarStart.getTime().getTime());
                resultMap.put(key, afterNode - beforeNode);
            }
            /**
             * 补全最后的数据
             */
            String lastKey = fastDateToFormat.format(calendarEnd.getTime());
            Long beforeNode = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, calendarStart.getTime().getTime());
            resultMap.put(lastKey, endOffset - beforeNode);
        } else if (endNode.before(firstNode)) {
            /**
             * 如果  endNode 在firstNode之前的（说明取值在节点之间）
             * -> 取提供发 end - first 归于 小的节点(endNode)
             * 类似 ___|__o___o___|___
             */
            String key = fastDateToFormat.format(DateUtils.truncate(timeEndDate, levelSimple.field));
            resultMap.put(key, endOffset - beginOffset);
        } else if (endNode.compareTo(firstNode) == 0) {
            /**
             * 如果  endNode 和 firstNode相等 （代表分别取前后的节点，这里的是两个节点）
             * ->  取值中间节点(大归大，小归小)
             * 类似 ____o____|____o_____
             */
            Long middleNodeOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, endNode.getTime());
            String firstKey = fastDateToFormat.format(DateUtils.truncate(timeStartDate, levelSimple.field));
            String endKey = fastDateToFormat.format(endNode);//取
            resultMap.put(firstKey, middleNodeOffset - beginOffset);
            resultMap.put(endKey, endOffset - middleNodeOffset);
        }
        consumerNoGroupService.getConsumer().close();
        return resultMap;
    }


    /**
     * 获取简单级别的画图(折线图)
     * 这里以时间来作为区分点！！！
     */
    private Map<String, Long> getRecordLineEChartsMap(String bootstrap_servers,
                                                      TopicPartition topicPartition,
                                                      Long timeStart,
                                                      Long timeEnd,
                                                      LevelSimple levelSimple, //画图的级别
                                                      Collection<Long> timeStamps//这个是额外需要插入的点

    ) throws ParseException {
        /**
         * 记录 beforeNode 和  afterNode 的差值， 并附在 before 的可以上
         */
        Map<String, Long> resultMap = new ConcurrentHashMap<>();
//        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.toFormat);
        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.SECONDS.toFormat);
        FastDateFormat fastMINUTESDateToFormat = FastDateFormat.getInstance(levelSimple.SECONDS.toFormat);//格式化 收尾使用
        if (null != timeStart && null != timeEnd && timeStart > timeEnd) {
            throw new RuntimeException("选择的start日期不能大于end日期:"
                    + "start:" + fastDateToFormat.format(new Date(timeStart))
                    + "end  :" + fastDateToFormat.format(new Date(timeEnd))
            );
        }
        /**
         * 获取一个消费者实例
         */
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        /**
         * 根据时间来限制范围
         */
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();

        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService
                = consumerFactory.getConsumerHavGroupAssignService(topicPartition);

        /**获取最早的时间*/
        OffsetAndTimestamp earliestRecordOffsetAndTimestamp = consumerHavGroupAssignService.getFirstPartitionOffsetAfterTimestamp(topicPartition, 0L);
        /**获取最晚的时间*/
        OffsetAndTimestamp latestRecordOffsetAndTimestamp = consumerHavGroupAssignService.getLastPartitionOffsetAndTimestamp(topicPartition);

        if (earliestRecordOffsetAndTimestamp == null) {
            return resultMap;
        }

        if (null != timeStart && earliestRecordOffsetAndTimestamp.timestamp() > timeStart) {
            /**
             * 选择出范围小的时间
             */
            timeStart = earliestRecordOffsetAndTimestamp.timestamp();
        } else if (null == timeStart) {
            timeStart = earliestRecordOffsetAndTimestamp.timestamp();
        }

        if (null != timeEnd && latestRecordOffsetAndTimestamp.timestamp() < timeEnd) {
            /**
             * 选择出范围小的时间
             */
            timeEnd = latestRecordOffsetAndTimestamp.timestamp();
        } else if (null == timeEnd) {
            timeEnd = latestRecordOffsetAndTimestamp.timestamp();
        }

        /**
         * !!!!开始和结束的区间！！！
         */
        Long beginOffset = this.getFirstOffsetAfterTimestamp(bootstrap_servers, topicPartition, timeStart);
        Long endOffset = this.getFirstOffsetAfterTimestamp(bootstrap_servers, topicPartition, timeEnd) + 1;
        //

        //这里已经算好了 开始和结束的时机 下面就是累计相加

        Date timeStartDate = new Date(timeStart);
        Date timeEndDate = new Date(timeEnd);


        Date firstNode = DateUtils.ceiling(timeStartDate, levelSimple.field);//有效的第一个节点
        Date endNode = DateUtils.truncate(timeEndDate, levelSimple.field);//有效的最后一个节点

        Calendar calendarStart = Calendar.getInstance();
        calendarStart.setTime(firstNode);
        Calendar calendarEnd = Calendar.getInstance();
        calendarEnd.setTime(endNode);


        if (endNode.after(firstNode)) {
            /**
             * 补全第一个的数据
             */
            String keyBeforeFirst = fastMINUTESDateToFormat.format(new Date(timeStart));//key取第一个节点
            resultMap.put(keyBeforeFirst, beginOffset);
            /**
             * 补全中间的节点
             */
            while (!calendarStart.after(calendarEnd)) {
                String key = fastDateToFormat.format(calendarStart.getTime());
                Long node = this.getFirstOffsetAfterTimestamp(bootstrap_servers, topicPartition, calendarStart.getTime().getTime());
                resultMap.put(key, node);
                /** 遍历 移动 直到和calendarEnd相同 */
                calendarStart.add(levelSimple.field, 1);//移到到下一个节点
            }

            /**
             * 补全最后一个的数据
             */
            String keyAfterEnd = fastMINUTESDateToFormat.format(new Date(timeEnd));//key取第一个节点
            resultMap.put(keyAfterEnd, endOffset);
        } else if (endNode.before(firstNode)) {
            /**
             * 如果  endNode 在firstNode之前的（说明取值在节点之间）
             * -> 取提供发 end - first 归于 小的节点(endNode)
             * 类似 ___|__o___o___|___
             */
            /**
             * 补全第一个的数据
             */
            String keyBeforeFirst = fastMINUTESDateToFormat.format(new Date(timeStart));//key取第一个节点
            resultMap.put(keyBeforeFirst, beginOffset);
            /**
             * 补全最后一个的数据
             */
            String keyAfterEnd = fastMINUTESDateToFormat.format(new Date(timeEnd));//key取第一个节点
            resultMap.put(keyAfterEnd, endOffset);
        } else if (endNode.compareTo(firstNode) == 0) {
            /**
             * 如果  endNode 和 firstNode相等 （代表分别取前后的节点，这里的是两个节点）
             * ->  取值中间节点(大归大，小归小)
             * 类似 ____o____|____o_____
             */
            /** 补全第一个的数据*/
            String keyBeforeFirst = fastMINUTESDateToFormat.format(new Date(timeStart));//key取第一个节点
            resultMap.put(keyBeforeFirst, beginOffset);
            /**补全最后一个的数据*/
            String keyAfterEnd = fastMINUTESDateToFormat.format(new Date(timeEnd));//key取第一个节点
            resultMap.put(keyAfterEnd, endOffset);
            /**补全中间节点*/
            Long middleNodeOffset = this.getFirstOffsetAfterTimestamp(bootstrap_servers, topicPartition, endNode.getTime());
            String key = fastDateToFormat.format(endNode.getTime());
            resultMap.put(key, middleNodeOffset);
        }

        /**
         * 补全插入的点(null就补0)
         */
        if (null != timeStamps) {
            timeStamps.parallelStream().forEach(timeStamp -> {
                String key = fastDateToFormat.format(timeStamp);
                OffsetAndTimestamp firstOffsetAndTimestampAfterTimestamp
                        = this.getFirstOffsetAndTimestampAfterTimestamp(bootstrap_servers, topicPartition, timeStamp);
                if (null == firstOffsetAndTimestampAfterTimestamp) {
                    resultMap.put(key, 0L);
                } else {
                    resultMap.put(key, firstOffsetAndTimestampAfterTimestamp.offset());
                }
            });
        }

        consumerNoGroupService.getConsumer().close();
        return resultMap;
    }

    /**
     * 根据 offset 查询 OffsetAndTimestamp(二分)
     * ！！！ 这里会主动减1 -> endOffsets 返回的是下一个的偏移量
     * !!!! 这里会创建很多消费者，但是会及时的关闭
     *
     * @param topicPartition
     * @param lastPartitionOffset
     * @return
     */
    public OffsetAndTimestamp getOffsetAndTimestampByOffset(String bootstrap_servers, TopicPartition topicPartition, Long lastPartitionOffset) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        OffsetAndTimestamp firstPartitionOffsetAndTimestamp = consumerFactory.getConsumerNoGroupService().getFirstPartitionOffsetAndTimestamp(topicPartition);
        consumerFactory.getKafkaConsumer().close();
        if (null == firstPartitionOffsetAndTimestamp) {
            /**如果第一个就为null,代表没有最后*/
            return null;
        }
        /**获取最后一个offset*/
        Long endTime = new Date().getTime();//最新的时间
        Long startTime = firstPartitionOffsetAndTimestamp.timestamp();
        OffsetAndTimestamp offsetAndTimestamp = null;
        do {
            ConsumerFactory<String, String> instance = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
            Long middle = (endTime + startTime) / 2;
            offsetAndTimestamp = instance.getConsumerNoGroupService().getFirstPartitionOffsetAfterTimestamp(topicPartition, middle);
            if (null == offsetAndTimestamp) {
                /**如过offsetAndTimestamp 为null -> 在右分 */
                endTime = middle;//左移

            } else if (offsetAndTimestamp.offset() < lastPartitionOffset) {
                /**如过offsetAndTimestamp < lastPartitionOffset  -> 在左分 */
                if (startTime == middle) {
                    log.error("异常发生:{}");
                }
                startTime = middle;//右移
            } else if (offsetAndTimestamp.offset() >= lastPartitionOffset) {
                /**如果 == 就退出 */
                break;
            }
            instance.getKafkaConsumer().close();
        } while (true);

        return offsetAndTimestamp;
    }

    /**
     * 获取 topicPartition 的指定时间最后的(用二分法) OffsetAndTimestamp
     * 这里 避免阻塞
     */
    public OffsetAndTimestamp getLastPartitionOffsetAndTimestamp(String bootstrap_servers, TopicPartition topicPartition) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        Long lastPartitionOffset = consumerFactory.getConsumerNoGroupService().getLastPartitionOffset(topicPartition);
        OffsetAndTimestamp offsetAndTimestamp = this.getOffsetAndTimestampByOffset(bootstrap_servers, topicPartition, lastPartitionOffset - 1);
        consumerFactory.getKafkaConsumer().close();
        return offsetAndTimestamp;

    }

    /**
     * 获取 topicPartition 的指定时间戳之后的第一个 offset
     */
    public Long getFirstOffsetAfterTimestamp(String bootstrap_servers, TopicPartition topicPartition, Long timestamp) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(topicPartition, timestamp);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = consumerFactory.getKafkaConsumer().offsetsForTimes(timestampsToSearch);
        consumerFactory.getKafkaConsumer().close();
        return topicPartitionOffsetAndTimestampMap.get(topicPartition).offset();
    }

    /**
     * 获取 topicPartition 的指定时间戳之后的第一个 offset和时间戳
     */
    public OffsetAndTimestamp getFirstOffsetAndTimestampAfterTimestamp(String bootstrap_servers, TopicPartition topicPartition, Long timestamp) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(topicPartition, timestamp);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap
                = consumerFactory.getKafkaConsumer().offsetsForTimes(timestampsToSearch);
        consumerFactory.getKafkaConsumer().close();
        return topicPartitionOffsetAndTimestampMap.get(topicPartition);
    }


}
