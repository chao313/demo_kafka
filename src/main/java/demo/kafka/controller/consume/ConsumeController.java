package demo.kafka.controller.consume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.service.AdminFactory;
import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.*;
import demo.kafka.controller.response.ConsumerTopicAndPartitionsAndOffset;
import demo.kafka.controller.response.EChartsVo;
import demo.kafka.controller.response.OffsetRecordResponse;
import demo.kafka.util.MapUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@Slf4j
@RequestMapping(value = "/ConsumeController")
@RestController
public class ConsumeController {


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
     * 1.加上正则
     */
    @ApiOperation(value = "获取每个分区的 最新和最早的offset")
    @GetMapping(value = "/getTopicPartitionAndRealOffsetList")
    public Object getTopicPartitionAndRealOffsetList(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic_pattern")
            @RequestParam(name = "topic_pattern", defaultValue = ".*")
                    String topic_pattern
    ) {


        ConsumerFactory<String, String> consumerFactory = ConsumerFactory.getInstance(bootstrap_servers, MapUtil.$());
        ConsumerNoGroupService<String, String> consumerNoGroupService = consumerFactory.getConsumerNoGroupService();
        List<ConsumerTopicAndPartitionsAndOffset> consumerTopicAndPartitionsAndOffsets = new ArrayList<>();

        Collection<TopicPartition> allTopicPartitions = consumerNoGroupService.getAllTopicPartitions();

        /**
         * 过滤正则
         */
        List<TopicPartition> filterCollect = allTopicPartitions.stream().filter(topicPartition -> {
            return topicPartition.topic().matches(topic_pattern);
        }).collect(Collectors.toList());

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
//            List<ConsumerRecord<String, String>> records
//                    = consumerCommonService.getRecord(bootstrap_servers, topicPartition, startOffset, endOffset - startOffset);
            FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
            Long timeStartTimeStamp = null;
            Long timeEndTimeStamp = null;
            if (StringUtils.isNotBlank(timeStart)) {
                timeStartTimeStamp = fastDateFormat.parse(timeStart).getTime();
            }
            if (StringUtils.isNotBlank(timeEnd)) {
                timeEndTimeStamp = fastDateFormat.parse(timeEnd).getTime();
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
            String JsonObject = new Gson().toJson(consumerRecords);
            JSONArray result = JSONObject.parseArray(JsonObject);
            return result;
        }


    }

    /**
     * 获取指定的offset(开始结束范围)的数据
     */
    @ApiOperation(value = "获取指定的offset(开始结束范围)的数据")
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
        EChartsVo recordECharts = consumerCommonService.getRecordSimpleECharts(bootstrap_servers,
                topicPartition,
                timeStartTimeStamp,
                timeEndTimeStamp,
                KafkaConsumerCommonService.LevelSimple.valueOf(level.toUpperCase()));
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


}
