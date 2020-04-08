package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminMetricService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminMetricTest {

    private static AdminMetricService adminMetricService = AdminMetricService.getInstance(Bootstrap.MY.getIp());


    @Test
    public void metrics() throws ExecutionException, InterruptedException {
        Map<MetricName, ? extends Metric> metricNameMap = adminMetricService.metrics();
        metricNameMap.forEach((key, value) -> {
            log.info("Metric.metricName:{}", value.metricName());
            log.info("Metric.metricValue:{}", value.metricValue());
        });

    }


    @Test
    public void metricGroupNameMap() throws ExecutionException, InterruptedException {
        Map<String, List<Metric>> metricNameMap = adminMetricService.metricGroupNameMap();
        metricNameMap.forEach((groupName, metrics) -> {
            log.info("groupName:{}", groupName);
            metrics.forEach(metric -> {
                log.info("groupName:{} -> metricName:{} -> value:{}", metric.metricName().group(), metric.metricName().name(), metric.metricValue());
            });
        });

    }


}
