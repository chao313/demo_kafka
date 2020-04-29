package demo.kafka.controller.consume.service;

import demo.kafka.controller.response.EChartsVo;
import demo.kafka.util.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import scala.collection.immutable.HashMapBuilder;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;
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
                if (keyRegexFlag && valueRegexFlag && record.offset() < endOffset) {
                    /**
                     * 全部符合要求
                     */
                    result.add(record);
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

        EChartsVo builder = EChartsVo.builder("msg消费图", "msg消费", "bar");

        builder.addXAxisData(resultMap.keySet());//添加x轴数据

        builder.addSeriesData(resultMap.values());//添加x轴数据

        instance.close();
        return builder.end();
    }

    public enum LevelSimple {
        YEAR("yyyy", "yyyy"),
        MONTH("yyyyMM", "yyyy-MM"),
        DAY("yyyyMMdd", "yyyy-MM-dd"),
        HOUR("yyyyMMddHH", "yyyy-MM-dd HH"),
        MINUTES("yyyyMMddHHmm", "yyyy-MM-dd HH:mm"),
        SECONDS("yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss"),
        MILLISECOND("yyyyMMddHHmmssS", "yyyy-MM-dd HH:mm:ss.S");
        private String format;
        private String toFormat;

        LevelSimple(String format, String toFormat) {
            this.format = format;
            this.toFormat = toFormat;
        }
    }

    /**
     * 获取简单级别的画图
     * 这里以时间来作为区分点！！！
     */
    public EChartsVo getRecordSimpleECharts(String bootstrap_servers,
                                            TopicPartition topicPartition,
                                            Long timeStart,
                                            Long timeEnd,
                                            LevelSimple levelSimple //画图的级别

    ) throws ParseException {

        /**
         * 参数有
         * 开始的时间
         * 结束的时间
         */


        Map<String, Long> resultMap = new HashMap<>();
        FastDateFormat fastDateFormat = FastDateFormat.getInstance(levelSimple.format);
        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.toFormat);

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
        ConsumerRecord<String, String> earliestRecord = consumerHavGroupAssignService.getEarliestRecord(topicPartition);
        /**获取最晚的时间*/
        ConsumerRecord<String, String> latestRecord = consumerHavGroupAssignService.getLatestRecord(topicPartition);

        if (null != timeStart && earliestRecord.timestamp() > timeStart) {
            /**
             * 选择出范围小的时间
             */
            timeStart = earliestRecord.timestamp();
        } else if (null == timeStart) {
            timeStart = earliestRecord.timestamp();
        }

        if (null != timeEnd && latestRecord.timestamp() < timeEnd) {
            /**
             * 选择出范围小的时间
             */
            timeEnd = latestRecord.timestamp();
        } else if (null == timeEnd) {
            timeEnd = latestRecord.timestamp();
        }

        //(指定区间)最开始的 offset
        Long startOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeStart);
        //(指定区间)最晚的 offset
        Long endOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeEnd);


        Long timeStartInt = Long.valueOf(fastDateFormat.format(timeStart));//开始的时间 -> 用作循环
        Long timeEndInt = Long.valueOf(fastDateFormat.format(timeEnd));//结束的时间 ->用作循环

        Map<Long, Long> timeToOffset = new HashMap<>();
        for (Long i = timeStartInt; i < timeEndInt; i++) {
            /**
             * 从格式化的前一个开始计算 但是不算 全部向前移动一位
             */
            OffsetAndTimestamp first
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, fastDateFormat.parse(String.valueOf(i)).getTime());
            OffsetAndTimestamp second
                    = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, fastDateFormat.parse(String.valueOf(i + 1)).getTime());

            if (i == timeStartInt) {
                /**
                 * 如果是第一个 计算开始到第一个的数量
                 */
                timeToOffset.put(i, second.offset() - startOffset);

            } else if (i == timeEndInt) {
                /**
                 * 如果是最后一个 计算开始到第一个的数量
                 */
                timeToOffset.put(i, endOffset - startOffset);

            } else {
                /**
                 * 获取时间节点的 偏移量
                 */
                timeToOffset.put(i, second.offset() - first.offset());
            }


        }
        for (Map.Entry<Long, Long> entry : timeToOffset.entrySet()) {
            Long time = entry.getKey();
            Long offset = entry.getValue();
            resultMap.put(fastDateToFormat.format(fastDateFormat.parse(String.valueOf(time)).getTime()), offset);
        }


        /**
         * 排序
         */
        resultMap = this.sortHashMap(resultMap);

        EChartsVo builder = EChartsVo.builder("msg图", "msg", "bar");

        builder.addXAxisData(resultMap.keySet());//添加x轴数据

        builder.addSeriesData(resultMap.values());//添加x轴数据

        consumerNoGroupService.getConsumer().close();
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


}
