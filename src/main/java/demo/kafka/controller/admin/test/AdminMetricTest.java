package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminMetricUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminMetricTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.MY.getIp());
        adminClient = AdminClient.create(properties);
    }


    @Test
    public void metrics() throws ExecutionException, InterruptedException {
        Map<MetricName, ? extends Metric> metricNameMap = AdminMetricUtil.metrics(adminClient);
        metricNameMap.forEach((key, value) -> {
            log.info("Metric.metricName:{}", value.metricName());
            log.info("Metric.metricValue:{}", value.metricValue());
        });

    }


    @Test
    public void metricGroupNameMap() throws ExecutionException, InterruptedException {
        Map<String, List<Metric>> metricNameMap = AdminMetricUtil.metricGroupNameMap(adminClient);
        metricNameMap.forEach((groupName, metrics) -> {
            log.info("groupName:{}", groupName);
            metrics.forEach(metric -> {
                log.info("groupName:{} -> metricName:{} -> value:{}", metric.metricName().group(), metric.metricName().name(), metric.metricValue());
            });
        });

    }


}
