package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import com.google.gson.Gson;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.service.AdminTopicService;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.*;
import demo.kafka.controller.response.*;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Slf4j
@RequestMapping(value = "/ConsumeController")
@RestController
public class ConsumeController {

    @Autowired
    private HttpSession session;

    @Autowired
    private RedisTemplate<String, LocalConsumerRecord<String, String>> redisTemplate;


    /**
     * 把偏移量设置到最早的 offset
     * 这里需要
     * 1.这里会引发再平衡(需要等待一段时间)
     * 2.kafkaManager会有一点延时,但是实际上已经完成
     */
    @ApiOperation(value = "指定消费者的offset设置到最开始", notes = "指定消费者的offset设置到最开始")
    @GetMapping(value = "/seekToBeginning")
    public void seekToBeginning(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要调拨的Topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "需要调拨的Topic的消费者groupId")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {
        ConsumerFactory<String, String> kafkaConsumer = ConsumerFactory.getInstance(bootstrap_servers, group_id);
        KafkaConsumer<String, String> consumer = kafkaConsumer.getKafkaConsumer();
        consumer.subscribe(Arrays.asList(topic));
        consumer.poll(10);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignments = consumer.assignment();
        consumer.seekToBeginning(assignments);
        consumer.poll(10);//必须要 poll一次才行(不然不会send到server端)
        consumer.wakeup();
    }

    /**
     *
     */
    @ApiOperation(value = "消费一次", notes = "消费一次")
    @GetMapping(value = "/listenerOnce")
    public void listenerOnce(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要消费的的Topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "需要消费的Topic的消费者groupId")
            @RequestParam(name = "group_id", defaultValue = "common_imp_db_test")
                    String group_id) {
        ConsumerFactory<String, String> consumerFactory
                = ConsumerFactory.getInstance(bootstrap_servers, group_id, MapUtil.$(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        KafkaConsumer<String, String> consumer = consumerFactory.getKafkaConsumer();
        KafkaConsumerSupService<String, String> kafkaConsumerSupService = KafkaConsumerSupService.getInstance(consumer);

        kafkaConsumerSupService.listenerOnce(Arrays.asList(topic), consumerRecord -> {
            log.info("offset:{} value:{}", consumerRecord.offset(), consumerRecord.value());
        });
    }


    /**
     *
     */
    @ApiOperation(value = "获取最新的Record", notes = "获取最新的Record")
    @GetMapping(value = "/getLastRecord")
    public void getLastRecord(
            @ApiParam(value = "kafka地址", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap_servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要消费的的Topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        KafkaConsumer<String, String> kafkaConsumer = consumerFactory.getKafkaConsumer();
        KafkaConsumerSupService<String, String> kafkaConsumerSupService = KafkaConsumerSupService.getInstance(kafkaConsumer);

        kafkaConsumerSupService.getLastRecordEachPartition(topic, consumerRecord -> {
            log.info("offset:{} value:{}", consumerRecord.offset(), consumerRecord.value());
        });
    }


//    @GetMapping(value = "/OffsetAndMetadata")
//    public void OffsetAndMetadata() {
//        KafkaConsumerService<String, String> consumerService = KafkaConsumerService.getProducerInstance(Bootstrap.HONE.getIp(), "test");
//        consumerService.subscribe(Arrays.asList("Test11"));
//        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
//        Set<TopicPartition> assignments = consumerService.assignment();
//
//        consumerService.poll(0);//必须要 poll一次才行(不然不会send到server端)
//        assignments.forEach(assignment -> {
//            OffsetAndMetadata offsetAndMetadata = consumerService.committed(assignment);
//            log.info("offsetAndMetadata:{}", offsetAndMetadata);
//        });
//    }

    @GetMapping(value = "/OffsetAndMetadata")
    public void OffsetAndMetadata() {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(Bootstrap.HONE.getIp(), "test");
        ConsumerFactory.getInstance(Bootstrap.HONE.getIp(), "test");
        KafkaConsumer<String, String> consumer = consumerFactory.getKafkaConsumer();
        consumer.subscribe(Arrays.asList("Test11"));
        consumer.poll(0);//必须要 poll一次才行(不然不会send到server端)
        Set<TopicPartition> assignments = consumer.assignment();

        consumer.poll(0);//必须要 poll一次才行(不然不会send到server端)
        assignments.forEach(assignment -> {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(assignment);
            log.info("offsetAndMetadata:{}", offsetAndMetadata);
        });
    }

    /**
     * 获取每个分区的 最新和最早的offset
     * 1.加上正则(修改为包含)
     */
    @ApiOperation(value = "获取每个分区的 最新和最早的offset")
    @GetMapping(value = "/getTopicPartitionAndRealOffsetList")
    public Object getTopicPartitionAndRealOffsetList(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topicContain")
            @RequestParam(name = "topicContain", defaultValue = "")
                    String topicContain
    ) throws ExecutionException, InterruptedException {

        List<ConsumerTopicAndPartitionsAndOffset> consumerTopicAndPartitionsAndOffsets
                = this.getConsumerTopicAndPartitionsAndOffset(bootstrap_servers, topicContain);

        return consumerTopicAndPartitionsAndOffsets;
    }

    /**
     * 获取全部分区的 最新和最早的offset
     * 1.加上正则(修改为包含)
     * 参考{@link #getTopicPartitionAndRealOffsetList(String, String)} 就是集合
     */
    @ApiOperation(value = "获取全部分区的 最新和最早的offset")
    @GetMapping(value = "/getTopicRealOffsetList")
    public Object getTopicRealOffsetList(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topicContain")
            @RequestParam(name = "topicContain", defaultValue = "")
                    String topicContain
    ) throws ExecutionException, InterruptedException {

        List<ConsumerTopicAndPartitionsAndOffset> consumerTopicAndPartitionsAndOffsets
                = this.getConsumerTopicAndPartitionsAndOffset(bootstrap_servers, topicContain);

        Map<String, ConsumerTopicOffset> resultMap = new HashMap<>();
        consumerTopicAndPartitionsAndOffsets.forEach(vo -> {
            if (resultMap.containsKey(vo.getTopic())) {
                ConsumerTopicOffset consumerTopicOffset = resultMap.get(vo.getTopic());
                consumerTopicOffset.setPartitions(consumerTopicOffset.getPartitions() + 1);
                consumerTopicOffset.setSum(consumerTopicOffset.getSum() + vo.getSum());
                consumerTopicOffset.setTotal(consumerTopicOffset.getTotal() + vo.getLastOffset());
                String earliestTimestampOld = consumerTopicOffset.getEarliestTimestamp();
                String earliestTimestampNew = vo.getEarliestTimestamp();
                if (StringUtils.isNotBlank(earliestTimestampNew) && StringUtils.isNotBlank(earliestTimestampOld)) {
                    consumerTopicOffset.setEarliestTimestamp(earliestTimestampNew.compareTo(earliestTimestampOld) > 0 ? earliestTimestampOld : earliestTimestampNew);
                } else if (StringUtils.isBlank(earliestTimestampNew)) {
                    consumerTopicOffset.setEarliestTimestamp(earliestTimestampOld);
                } else if (StringUtils.isBlank(earliestTimestampOld)) {
                    consumerTopicOffset.setEarliestTimestamp(earliestTimestampNew);
                }
                resultMap.put(vo.getTopic(), consumerTopicOffset);

            } else {
                ConsumerTopicOffset consumerTopicOffset = new ConsumerTopicOffset();
                consumerTopicOffset.setTopic(vo.getTopic());
                consumerTopicOffset.setEarliestTimestamp(vo.getEarliestTimestamp());
                consumerTopicOffset.setSum(vo.getSum());
                consumerTopicOffset.setPartitions(1);
                consumerTopicOffset.setTotal(vo.getLastOffset());
                resultMap.put(consumerTopicOffset.getTopic(), consumerTopicOffset);
            }
        });

        Collection<ConsumerTopicOffset> resultSet = resultMap.values();
        List<ConsumerTopicOffset> resultList = new ArrayList<>(resultSet);
        /**
         * 排序
         */
        Collections.sort(resultList, new Comparator<ConsumerTopicOffset>() {
            @Override
            public int compare(ConsumerTopicOffset o1, ConsumerTopicOffset o2) {
                return o1.getTopic().compareTo(o2.getTopic());
            }
        });

        return resultList;

    }

    /**
     * 获取每个分区的 最新和最早的offset
     * 1.加上正则(修改为包含)
     */
    @ApiOperation(value = "获取每个分区的 最新和最早的offset(精确查询)")
    @GetMapping(value = "/getTopicPartitionAndRealOffsetListByTopicRegex")
    public Object getTopicPartitionAndRealOffsetListByTopicRegex(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topicRegex")
            @RequestParam(name = "topicRegex", defaultValue = "")
                    String topicRegex
    ) {

        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        List<ConsumerTopicAndPartitionsAndOffset> consumerTopicAndPartitionsAndOffsets = new ArrayList<>();

        Collection<TopicPartition> allTopicPartitions = consumerNoGroupService.getAllTopicPartitions();

        /**
         * 过滤包含
         */
        List<TopicPartition> filterCollect = new ArrayList<>();
        if (StringUtils.isNotBlank(topicRegex)) {
            filterCollect = allTopicPartitions.stream().filter(topicPartition -> {
                return topicPartition.topic().matches(topicRegex);
            }).collect(Collectors.toList());
        } else {
            filterCollect.addAll(allTopicPartitions);
        }

        /**
         * 获取最早和最晚的offset
         */
        Map<TopicPartition, Long> beginningOffsets = consumerNoGroupService.getConsumer().beginningOffsets(filterCollect);
        Map<TopicPartition, Long> endOffsets = consumerNoGroupService.getConsumer().endOffsets(filterCollect);


        filterCollect.forEach(topicPartition -> {
            ConsumerTopicAndPartitionsAndOffset vo = new ConsumerTopicAndPartitionsAndOffset();
            vo.setTopic(topicPartition.topic());
            vo.setPartition(topicPartition.partition());
            vo.setEarliestOffset(beginningOffsets.get(topicPartition));
            vo.setLastOffset(endOffsets.get(topicPartition));
            vo.setSum(vo.getLastOffset() - vo.getEarliestOffset());
            consumerTopicAndPartitionsAndOffsets.add(vo);
        });
        /**
         * 排序
         */
        Collections.sort(consumerTopicAndPartitionsAndOffsets, new Comparator<ConsumerTopicAndPartitionsAndOffset>() {
            @Override
            public int compare(ConsumerTopicAndPartitionsAndOffset o1, ConsumerTopicAndPartitionsAndOffset o2) {

                if (0 != o1.getTopic().compareTo(o2.getTopic())) {
                    return o1.getTopic().compareTo(o2.getTopic());
                } else {
                    return o1.getPartition() - o2.getPartition();
                }
            }
        });
        consumerFactory.getKafkaConsumer().close();
        return consumerTopicAndPartitionsAndOffsets;
    }


    /**
     * 获取partition的详细的信息
     */
    @ApiOperation(value = "获取 partition 详情")
    @GetMapping(value = "/getTopicPartitionAndRealOffsetDetail")
    public Object getTopicPartitionAndRealOffsetDetail(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition

    ) {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());


        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService
                = consumerFactory.getConsumerHavGroupAssignService(topic, partition);

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long earliestPartitionOffset = consumerHavGroupAssignService.getEarliestPartitionOffset(topicPartition);
        Long lastPartitionOffset = consumerHavGroupAssignService.getLastPartitionOffset(topicPartition);
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        ConsumerRecord earliestOneRecord = consumerCommonService.getOneRecord(bootstrap_servers, topicPartition, earliestPartitionOffset);
        ConsumerRecord lastOneRecord = consumerCommonService.getOneRecord(bootstrap_servers, topicPartition, lastPartitionOffset > 0 ? lastPartitionOffset - 1 : 0);

        /**
         * 获取最后10条记录的开始的offset
         */
        long offsetStart;
        if ((lastPartitionOffset - earliestPartitionOffset) >= 10) {
            offsetStart = lastPartitionOffset - 10;
        } else {
            offsetStart = earliestPartitionOffset;
        }
        List<ConsumerRecord> lastTenRecords = consumerCommonService.getRecord(bootstrap_servers, topicPartition, offsetStart, 10);

        List<ConsumerRecord> consumerRecords = new ArrayList<>();
        consumerRecords.addAll(lastTenRecords);
        /**
         * 排序
         */
        Collections.sort(consumerRecords, new Comparator<ConsumerRecord>() {
            @Override
            public int compare(ConsumerRecord o1, ConsumerRecord o2) {
                return Long.valueOf(o2.offset() - o1.offset()).intValue();
            }
        });
        /**
         * 转换返回
         */
        ConsumerTopicAndPartitionsAndOffset vo = new ConsumerTopicAndPartitionsAndOffset();
        vo.setLastOffset(lastPartitionOffset);
        vo.setEarliestOffset(earliestPartitionOffset);
        vo.setTopic(topic);
        vo.setPartition(partition);
        vo.setSum(vo.getLastOffset() - vo.getEarliestOffset());
        vo.setLastConsumerRecord(lastOneRecord);
        vo.setEarliestConsumerRecord(earliestOneRecord);
        vo.setLastTenConsumerRecords(consumerRecords);

        String JsonObject = new Gson().toJson(vo);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;

    }

    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "获取指定的offset(开始结束范围)的数据")
    @GetMapping(value = "/getRecordByTopicPartitionOffset")
    public Object getRecordByTopicPartitionOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition,
            @ApiParam(value = "开始的offset")
            @RequestParam(name = "startOffset", defaultValue = "0")
                    int startOffset,
            @ApiParam(value = "结束的offset")
            @RequestParam(name = "endOffset", defaultValue = "0")
                    int endOffset,
            @ApiParam(value = "key的正则")
            @RequestParam(name = "keyRegex", defaultValue = "")
                    String keyRegex,
            @ApiParam(value = "value的正则")
            @RequestParam(name = "valueRegex", defaultValue = "")
                    String valueRegex,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd
    ) throws ParseException {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService =
                consumerFactory.getConsumerHavGroupAssignService(topic, partition);

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long earliestPartitionOffset = consumerHavGroupAssignService.getEarliestPartitionOffset(topicPartition);
        Long lastPartitionOffset = consumerHavGroupAssignService.getLastPartitionOffset(topicPartition);
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();

        if (endOffset <= startOffset) {
            throw new RuntimeException("endOffset应该>startOffset");
        }

        if (startOffset < earliestPartitionOffset) {
            throw new RuntimeException("startOffset 应该>最早有效的offset:" + earliestPartitionOffset);
        }

        if (endOffset > lastPartitionOffset) {
            throw new RuntimeException("endOffset 应该<最新的offset:" + lastPartitionOffset);
        }

        if (topic.equalsIgnoreCase(KafkaConsumerCommonService.__consumer_offsets)) {
            /**
             * __consumer_offsets 专享
             */

            List<ConsumerRecord<byte[], byte[]>> records
                    = consumerCommonService.getRecord(bootstrap_servers, topicPartition, startOffset, endOffset - startOffset,
                    MapUtil.$(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
            );


            List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
            consumerRecords.addAll(records);
            /**
             * 排序
             */
            Collections.sort(consumerRecords, new Comparator<ConsumerRecord>() {
                @Override
                public int compare(ConsumerRecord o1, ConsumerRecord o2) {
                    return Long.valueOf(o2.offset() - o1.offset()).intValue();
                }
            });

            List<OffsetRecordResponse> list = OffsetRecordResponse.getList(consumerRecords);

            String JsonObject = new Gson().toJson(list);
            JSONArray result = JSONObject.parseArray(JsonObject);
            return result;
        } else {
            FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
            Long timeStartTimeStamp = null;
            Long timeEndTimeStamp = null;
            if (StringUtils.isNotBlank(timeStart)) {
                timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
            }
            if (StringUtils.isNotBlank(timeEnd)) {
                timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
                /**
                 * 这里是区间 -> :xx:xx:38 其实应该是39截止 -> 多加1秒
                 */
                timeEndTimeStamp = DateUtils.addSeconds(new Date(timeEndTimeStamp), 1).getTime();
            }
            List<ConsumerRecord<String, String>> records
                    = consumerCommonService.getRecord(bootstrap_servers,
                    topicPartition,
                    startOffset,
                    endOffset,
                    keyRegex,
                    valueRegex,
                    timeStartTimeStamp,
                    timeEndTimeStamp);
            List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
            consumerRecords.addAll(records);
            /**
             * 排序
             */
            Collections.sort(consumerRecords, new Comparator<ConsumerRecord>() {
                @Override
                public int compare(ConsumerRecord o1, ConsumerRecord o2) {
                    return Long.valueOf(o2.offset() - o1.offset()).intValue();
                }
            });
            log.info("consumerRecords的数量:{}", consumerRecords.size());
            List<LocalConsumerRecord<String, String>> changeResult = LocalConsumerRecord.change(consumerRecords);
            log.info("changeResult的数量:{}", changeResult.size());
            String uuid = UUID.randomUUID().toString();
            redisTemplate.opsForList().leftPushAll(uuid, changeResult);
            return this.getRecordByScrollId(uuid, 1, 10);
        }


    }

    @ApiOperation(value = "根据ScrollId获取数据")
    @GetMapping(value = "/getRecordByScrollId")
    public Object getRecordByScrollId(
            @RequestParam(name = "scrollId", defaultValue = "")
                    String scrollId,
            @RequestParam(value = "pageNum", defaultValue = "1", required = false)
                    Integer pageNum,
            @RequestParam(value = "pageSize", defaultValue = "10", required = false)
                    Integer pageSize
    ) {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");
        PageInfo pageInfo = null;
        if (StringUtils.isNotBlank(scrollId)) {
            Long size = redisTemplate.opsForList().size(scrollId);
            Long end = Long.valueOf(pageNum * pageSize) - 1;
            if (pageNum * pageSize > size) {
                end = size;
            }
            List list = redisTemplate.opsForList().range(scrollId, (pageNum - 1) * pageSize, end);
            Page page = new Page(pageNum, pageSize, false);
            page.setTotal(size);
            page.setOrderBy(scrollId);
            page.addAll(list);
            pageInfo = new PageInfo(page);
            /**
             * 更新有效时间
             * 优化超时时间的设置 原来是 redisTemplate.opsForValue().set(uuid, changeResult, 5, TimeUnit.MINUTES);
             */
            redisTemplate.expire(scrollId, 5, TimeUnit.MINUTES);
        } else {
            throw new RuntimeException("scrollID已经失效,请点击查询按键，重新查询");
        }
        String JsonObject = new Gson().toJson(pageInfo);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }


    @ApiOperation(value = "获取柱状图(Partition级别)")
    @GetMapping(value = "/getRecordEChartsByTopicPartitionOffset")
    public Object getRecordEChartsByTopicPartitionOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition,
            @ApiParam(value = "开始的offset")
            @RequestParam(name = "startOffset", defaultValue = "0")
                    int startOffset,
            @ApiParam(value = "结束的offset")
            @RequestParam(name = "endOffset", defaultValue = "0")
                    int endOffset,
            @ApiParam(value = "key的正则")
            @RequestParam(name = "keyRegex", defaultValue = "")
                    String keyRegex,
            @ApiParam(value = "value的正则")
            @RequestParam(name = "valueRegex", defaultValue = "")
                    String valueRegex,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerHavGroupAssignService<String, String> consumerHavGroupAssignService =
                consumerFactory.getConsumerHavGroupAssignService(topic, partition);

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Long earliestPartitionOffset = consumerHavGroupAssignService.getEarliestPartitionOffset(topicPartition);
        Long lastPartitionOffset = consumerHavGroupAssignService.getLastPartitionOffset(topicPartition);
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();

        if (endOffset <= startOffset) {
            throw new RuntimeException("endOffset应该>startOffset");
        }

        if (startOffset < earliestPartitionOffset) {
            throw new RuntimeException("startOffset 应该>最早有效的offset:" + earliestPartitionOffset);
        }

        if (endOffset > lastPartitionOffset) {
            throw new RuntimeException("endOffset 应该<最新的offset:" + lastPartitionOffset);
        }


        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }

        EChartsVo recordECharts = consumerCommonService.getRecordECharts(bootstrap_servers,
                topicPartition,
                startOffset,
                endOffset,
                keyRegex,
                valueRegex,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.Level.valueOf(level.toUpperCase()));


        String JsonObject = new Gson().toJson(recordECharts);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }

    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "获取指定的Topic的数据(Topic级别的)")
    @GetMapping(value = "/getRecordByTopic")
    public Object getRecordByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "key的正则")
            @RequestParam(name = "keyRegex", defaultValue = "")
                    String keyRegex,
            @ApiParam(value = "value的正则")
            @RequestParam(name = "valueRegex", defaultValue = "")
                    String valueRegex,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd
    ) throws ParseException {
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        /**获取全部的 topicPartition*/
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        Collection<TopicPartition> topicPartitions
                = consumerFactory.getConsumerNoGroupService().getTopicPartitionsByTopic(topic);

        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
            /**
             * 这里是区间 -> :xx:xx:38 其实应该是39截止 -> 多加1秒
             */
            timeEndTimeStamp = DateUtils.addSeconds(new Date(timeEndTimeStamp), 1).getTime();
        }
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        Long finalTimeStartTimeStamp = timeStartTimeStamp;
        Long finalTimeEndTimeStamp = timeEndTimeStamp;
        topicPartitions.parallelStream().forEach(topicPartition -> {
            List<ConsumerRecord<String, String>> records
                    = consumerCommonService.getRecord(bootstrap_servers,
                    topicPartition,
                    keyRegex,
                    valueRegex,
                    finalTimeStartTimeStamp,
                    finalTimeEndTimeStamp);
            result.addAll(records);
        });

        /**
         * 排序
         */
        Collections.sort(result, new Comparator<ConsumerRecord>() {
            @Override
            public int compare(ConsumerRecord o1, ConsumerRecord o2) {
                return Long.valueOf(o2.timestamp() - o1.timestamp()).intValue();
            }
        });
        List<LocalConsumerRecord<String, String>> changeResult = LocalConsumerRecord.change(result);
        String uuid = UUID.randomUUID().toString();
        redisTemplate.opsForList().leftPushAll(uuid, changeResult);
        return this.getRecordByScrollId(uuid, 1, 10);
    }


    @ApiOperation(value = "获取柱状图(topic级别)")
    @GetMapping(value = "/getRecordEChartsByTopic")
    public Object getRecordEChartsByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "key的正则")
            @RequestParam(name = "keyRegex", defaultValue = "")
                    String keyRegex,
            @ApiParam(value = "value的正则")
            @RequestParam(name = "valueRegex", defaultValue = "")
                    String valueRegex,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        Collection<TopicPartition> topicPartitions = consumerFactory.getConsumerNoGroupService().getTopicPartitionsByTopic(topic);


        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }

        Long finalTimeStartTimeStamp = timeStartTimeStamp;
        Long finalTimeEndTimeStamp = timeEndTimeStamp;
        Map<String, Long> resultMap = new ConcurrentHashMap<>();//线程安全
        Map<String, Long> finalResultMap = resultMap;
        topicPartitions.parallelStream().forEach(topicPartition -> {
            Map<String, Long> resultTmp = consumerCommonService.getRecordEChartsMap(
                    bootstrap_servers,
                    topicPartition,
                    keyRegex,
                    valueRegex,
                    finalTimeStartTimeStamp,
                    finalTimeEndTimeStamp,
                    KafkaConsumerCommonService.Level.valueOf(level.toUpperCase()));
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
        });
        //排序
        Map map = consumerCommonService.sortHashMap(finalResultMap);
        EChartsVo builder = EChartsVo.builder("bar");
        builder.addXAxisData(map.keySet());//添加x轴数据
        builder.addSeriesData(map.values());//添加x轴数据
        String JsonObject = new Gson().toJson(builder.end());
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }


    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "获取指定的offset(开始结束范围)的数据（简单的，没有key和value的过滤）")
    @GetMapping(value = "/getRecordSimpleEChartsByTopicPartitionOffset")
    public Object getRecordSimpleEChartsByTopicPartitionOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();


        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }
        EChartsVo recordECharts = consumerCommonService.getRecordTopicPartitionSimpleECharts(bootstrap_servers,
                topicPartition,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.LevelSimple.valueOf(level.toUpperCase()));
        String JsonObject = new Gson().toJson(recordECharts);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }

    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "(Topic级别的)获取指定的offset(开始结束范围)的数据（简单的，没有key和value的过滤）")
    @GetMapping(value = "/getRecordTopicSimpleEChartsByTopicOffset")
    public Object getRecordTopicSimpleEChartsByTopicOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        Collection<TopicPartition> topicPartitionsByTopic = consumerFactory.getConsumerNoGroupService().getTopicPartitionsByTopic(topic);

        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }
        EChartsVo recordECharts = consumerCommonService.getRecordTopicSimpleECharts(bootstrap_servers,
                topicPartitionsByTopic,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.LevelSimple.valueOf(level.toUpperCase()));
        String JsonObject = new Gson().toJson(recordECharts);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }

    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "获取指定的offset(开始结束范围)的数据（简单的，没有key和value的过滤）")
    @GetMapping(value = "/getRecordLineEChartsByTopicPartitionOffset")
    public Object getRecordLineEChartsByTopicPartitionOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @RequestParam(name = "partition", defaultValue = "0")
                    int partition,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();


        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }
        LineEChartsVo recordECharts = consumerCommonService.getRecordLineECharts(bootstrap_servers,
                topicPartition,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.LevelSimple.valueOf(level.toUpperCase()));
        String JsonObject = new Gson().toJson(recordECharts);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }

    /**
     * 获取指定的offset(开始结束范围)的数据 (整个topic的)
     */
    @ApiOperation(value = "获取指定的offset(开始结束范围)的数据（简单的，没有key和value的过滤）(topic级别的)")
    @GetMapping(value = "/getRecordLineEChartsByTopic")
    public Object getRecordLineEChartsByTopic(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "需要查询的 topic")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "消息start的时间")
            @RequestParam(name = "timeStart", defaultValue = "")
                    String timeStart,
            @ApiParam(value = "消息end的时间")
            @RequestParam(name = "timeEnd", defaultValue = "")
                    String timeEnd,
            @ApiParam(value = "画图的级别")
            @RequestParam(name = "level", defaultValue = "DAY")
                    String level
    ) throws ParseException {
        KafkaConsumerCommonService consumerCommonService = new KafkaConsumerCommonService();
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        Collection<TopicPartition> topicPartitions = consumerNoGroupService.getTopicPartitionsByTopic(topic);
        /**补全需要插入的节点*/
        Set<Long> timeStamps = ConcurrentHashMap.newKeySet(10);

        for (TopicPartition topicPartition : topicPartitions) {/**获取最早的时间*/
            OffsetAndTimestamp earliestRecordOffsetAndTimestamp = consumerNoGroupService.getFirstPartitionOffsetAfterTimestamp(topicPartition, 0L);
            /**获取最晚的时间*/
            OffsetAndTimestamp latestRecordOffsetAndTimestamp = consumerNoGroupService.getLastPartitionOffsetAndTimestamp(topicPartition);
            if (null != earliestRecordOffsetAndTimestamp) {
                timeStamps.add(earliestRecordOffsetAndTimestamp.timestamp());
            }
            if (null != latestRecordOffsetAndTimestamp) {
                timeStamps.add(latestRecordOffsetAndTimestamp.timestamp());
            }
        }


        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        Long timeStartTimeStamp = null;
        Long timeEndTimeStamp = null;
        if (StringUtils.isNotBlank(timeStart)) {
            timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
        }
        if (StringUtils.isNotBlank(timeEnd)) {
            timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
        }
        LineEChartsVo recordECharts = consumerCommonService.getRecordLineECharts(bootstrap_servers,
                topicPartitions,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.LevelSimple.valueOf(level.toUpperCase()), timeStamps);
        String JsonObject = new Gson().toJson(recordECharts);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;


    }


    /**
     * 把 指定 到的 partition 更新到 指定的偏移量
     */
    @ApiOperation(value = "把 分配 到的 partition 全部更新到 指定的偏移量")
    @GetMapping(value = "/updatePartitionToOffset")
    public Object updatePartitionAssignedOffset(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "指定的 offset")
            @RequestParam(name = "group.id")
                    String group_id,
            @ApiParam(value = "指定的 topic")
            @RequestParam(name = "topic", defaultValue = "1")
                    String topic,
            @ApiParam(value = "指定的 partition")
            @RequestParam(name = "partition", defaultValue = "1")
                    int partition,
            @ApiParam(value = "指定的 offset")
            @RequestParam(name = "seekOffset", defaultValue = "1")
                    long seekOffset
    ) throws ExecutionException, InterruptedException {
        /**
         * seek前判断 : consumer的状态必须是 empty才行
         */
        ConsumerGroupDescription consumerGroupDescribe =
                AdminFactory.getAdminConsumerGroupsService(bootstrap_servers).getConsumerGroupDescribe(group_id);

        if (!consumerGroupDescribe.state().equals(ConsumerGroupState.EMPTY)) {
            throw new RuntimeException("当前消费者的状态不是EMPTY,无法seek --> 当前状态是:" + consumerGroupDescribe.state());
        }
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, group_id, MapUtil.$());
        ConsumerHavGroupSubscribeService<String, String> instance = consumerFactory.getConsumerHavGroupSubscribeService(Arrays.asList(topic));
        instance.getConsumer().poll(0);
        instance.updatePartitionSubscribedOffset(new TopicPartition(topic, partition), seekOffset);
        instance.getConsumer().poll(0);
        instance.getConsumer().close();
        return "调整结束";
    }

    /**
     * 抽取出来的函数
     *
     * @param bootstrap_servers
     * @param topicContain
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private List<ConsumerTopicAndPartitionsAndOffset> getConsumerTopicAndPartitionsAndOffset(String bootstrap_servers, String topicContain) throws ExecutionException, InterruptedException {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.S");
        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        List<ConsumerTopicAndPartitionsAndOffset> consumerTopicAndPartitionsAndOffsets = new ArrayList<>();

        AdminTopicService adminTopicService = AdminFactory.getAdminTopicService(bootstrap_servers);
        Collection<String> topics = adminTopicService.getTopicNames();
        Collection<TopicPartition> allTopicPartitions = consumerNoGroupService.getTopicPartitionsByTopic(topics);
        /**
         * 过滤包含
         */
        List<TopicPartition> filterCollect = new ArrayList<>();
        if (StringUtils.isNotBlank(topicContain)) {
            filterCollect = allTopicPartitions.stream().filter(topicPartition -> {
                return topicPartition.topic().contains(topicContain);
            }).collect(Collectors.toList());
        } else {
            filterCollect.addAll(allTopicPartitions);
        }

        /**
         * 获取最早和最晚的offset
         */
        Map<TopicPartition, Long> beginningOffsets = consumerNoGroupService.getConsumer().beginningOffsets(filterCollect);
        Map<TopicPartition, Long> endOffsets = consumerNoGroupService.getConsumer().endOffsets(filterCollect);

        Map<TopicPartition, OffsetAndTimestamp>
                beginningTimestampMap = new HashMap<>();

        filterCollect.forEach(topicPartition -> {
            ConsumerTopicAndPartitionsAndOffset vo = new ConsumerTopicAndPartitionsAndOffset();
            vo.setTopic(topicPartition.topic());
            vo.setPartition(topicPartition.partition());
            vo.setEarliestOffset(beginningOffsets.get(topicPartition));
            vo.setLastOffset(endOffsets.get(topicPartition));
            vo.setSum(vo.getLastOffset() - vo.getEarliestOffset());
            consumerTopicAndPartitionsAndOffsets.add(vo);
        });
        /**
         * 排序
         */
        Collections.sort(consumerTopicAndPartitionsAndOffsets, new Comparator<ConsumerTopicAndPartitionsAndOffset>() {
            @Override
            public int compare(ConsumerTopicAndPartitionsAndOffset o1, ConsumerTopicAndPartitionsAndOffset o2) {

                if (0 != o1.getTopic().compareTo(o2.getTopic())) {
                    return o1.getTopic().compareTo(o2.getTopic());
                } else {
                    return o1.getPartition() - o2.getPartition();
                }
            }
        });
        consumerFactory.getKafkaConsumer().close();
        return consumerTopicAndPartitionsAndOffsets;
    }


}
