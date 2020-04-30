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
        MINUTES("yyyyMMddHHmm", "yyyy-MM-dd HH:mm", Calendar.SECOND),
        SECONDS("yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", Calendar.MILLISECOND),
        MILLISECOND("yyyyMMddHHmmssS", "yyyy-MM-dd HH:mm:ss.S", Calendar.ZONE_OFFSET);
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
     * 获取简单级别的画图
     * 这里以时间来作为区分点！！！
     */
    public EChartsVo getRecordSimpleECharts(String bootstrap_servers,
                                            TopicPartition topicPartition,
                                            Long timeStart,
                                            Long timeEnd,
                                            LevelSimple levelSimple //画图的级别

    ) throws ParseException {
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

        /**
         * !!!!开始和结束的区间！！！
         */
        Long beginOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeStart);
        Long endOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeEnd);
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

        /**
         * 记录 beforeNode 和  afterNode 的差值， 并附在 before 的可以上
         */
        Map<String, Long> resultMap = new HashMap<>();


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
            String endKey = fastDateToFormat.format(DateUtils.ceiling(timeEndDate, levelSimple.field));//取
            resultMap.put(firstKey, middleNodeOffset - beginOffset);
            resultMap.put(endKey, endOffset - middleNodeOffset);
        }
        /**
         * 排序
         */
        resultMap = this.sortHashMap(resultMap);

        EChartsVo builder = EChartsVo.builder("bar");

        builder.addXAxisData(resultMap.keySet());//添加x轴数据

        builder.addSeriesData(resultMap.values());//添加x轴数据

        consumerNoGroupService.getConsumer().close();
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
//        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.toFormat);
        FastDateFormat fastDateToFormat = FastDateFormat.getInstance(levelSimple.MINUTES.toFormat);
        FastDateFormat fastMINUTESDateToFormat = FastDateFormat.getInstance(levelSimple.MINUTES.toFormat);//格式化 收尾使用
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

        /**
         * !!!!开始和结束的区间！！！
         */
        Long beginOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeStart);
        Long endOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, timeEnd);
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

        /**
         * 记录 beforeNode 和  afterNode 的差值， 并附在 before 的可以上
         */
        Map<String, Long> resultMap = new HashMap<>();


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
                Long node = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, calendarStart.getTime().getTime());
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
            Long middleNodeOffset = consumerNoGroupService.getFirstOffsetAfterTimestamp(topicPartition, endNode.getTime());
            String key = fastDateToFormat.format(endNode.getTime());
            resultMap.put(key, middleNodeOffset);
        }
        /**
         * 排序
         */
        resultMap = this.sortHashMap(resultMap);

        LineEChartsVo builder = LineEChartsVo.builder();

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
