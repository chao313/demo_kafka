package demo.kafka.controller.produce.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.produce.service.KafkaProduceDefaultService;
import demo.kafka.controller.produce.service.KafkaProduceService;
import demo.kafka.controller.produce.service.ProduceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * 专门测试 send
 */
@Slf4j
public class KafkaProduceServiceTest {

    /**
     * 测试 获取分区信息 getPartitionsByTopic
     */
    @Test
    public void partitionsFor() {
        List<PartitionInfo> list
                = ProduceFactory.getProducerInstance(Bootstrap.MY.getIp()).getKafkaProduceDefaultService().getPartitionsByTopic("Test");

        list.forEach(partitionInfo -> {
            log.info("首领分区在的节点  : partitionInfo.leader:{}", partitionInfo.leader());
            log.info("当前分区的    id : partitionInfo.partition:{}", partitionInfo.partition());
            log.info("当前分区的 Topic : partitionInfo.topic:{}", partitionInfo.topic());
            log.info("同步复制子集     : partitionInfo.inSyncReplicas:{}", partitionInfo.inSyncReplicas());
            log.info("完整子集:        : partitionInfo.replicas:{}", partitionInfo.replicas());
            log.info("离线副本         : partitionInfo.offlineReplicas:{}", partitionInfo.offlineReplicas());
        });
    }

    /**
     * 测试 获取评估
     */
    @Test
    public void metricGroupNameMap() {
        Map<String, List<Metric>> metricNameMap =
                ProduceFactory.getProducerInstance(Bootstrap.HONE.getIp()).getKafkaProduceDefaultService().metricGroupNameMap();

        metricNameMap.forEach((groupName, metrics) -> {
            log.info("groupName:{}", groupName);
            metrics.forEach(metric -> {
                log.info("groupName:{} -> metricName:{} -> value:{}", metric.metricName().group(), metric.metricName().name(), metric.metricValue());
            });
        });
    }


}
