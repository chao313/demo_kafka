package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * 度量
 */
public class AdminMetricUtil {

    public static Map<MetricName, ? extends Metric> metrics(AdminClient client) throws ExecutionException, InterruptedException {
        Map<MetricName, ? extends Metric> metricNameMap = client.metrics();
        return metricNameMap;
    }

    /**
     * 重新根据 group 来分组
     * <p>
     * groupName:kafka-metrics-count
     * groupName:kafka-metrics-count -> metricName:count -> value:40.0
     * <p>
     * groupName:admin-client-metrics
     * groupName:admin-client-metrics -> metricName:io-waittime-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:request-size-max -> value:NaN
     * groupName:admin-client-metrics -> metricName:failed-authentication-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:failed-reauthentication-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:failed-reauthentication-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:connection-close-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:reauthentication-latency-max -> value:NaN
     * groupName:admin-client-metrics -> metricName:connection-close-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:successful-authentication-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:connection-creation-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:request-size-avg -> value:NaN
     * groupName:admin-client-metrics -> metricName:connection-count -> value:1.0
     * groupName:admin-client-metrics -> metricName:successful-reauthentication-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:outgoing-byte-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:select-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:response-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:select-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:network-io-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:connection-creation-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:failed-authentication-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:io-wait-time-ns-avg -> value:NaN
     * groupName:admin-client-metrics -> metricName:io-time-ns-avg -> value:NaN
     * groupName:admin-client-metrics -> metricName:successful-authentication-no-reauth-total -> valu
     * groupName:admin-client-metrics -> metricName:request-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:request-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:response-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:network-io-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:incoming-byte-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:reauthentication-latency-avg -> value:NaN
     * groupName:admin-client-metrics -> metricName:io-wait-ratio -> value:0.0
     * groupName:admin-client-metrics -> metricName:iotime-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:io-ratio -> value:0.0
     * groupName:admin-client-metrics -> metricName:successful-reauthentication-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:outgoing-byte-total -> value:0.0
     * groupName:admin-client-metrics -> metricName:successful-authentication-rate -> value:0.0
     * groupName:admin-client-metrics -> metricName:incoming-byte-total -> value:0.0
     * <p>
     * groupName:app-info
     * groupName:app-info -> metricName:start-time-ms -> value:1585975635008
     * groupName:app-info -> metricName:commit-id -> value:18a913733fb71c01
     * groupName:app-info -> metricName:version -> value:2.3.1
     *
     * @param client
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Map<String, List<Metric>> metricGroupNameMap(AdminClient client) throws ExecutionException, InterruptedException {
        Map<MetricName, ? extends Metric> metricNameMap = AdminMetricUtil.metrics(client);
        Map<String, List<Metric>> metricGroupNameMap = new HashMap<>();
        metricNameMap.forEach((name, metric) -> {
            String groupName = metric.metricName().group();
            if (!metricGroupNameMap.containsKey(groupName)) {
                /**
                 * 如果不存在就 新建List
                 * 注意！ Arrays.asList 和 ArrayList不是同一个
                 */
                List<Metric> metrics = new ArrayList<>(Arrays.asList(metric));
                metricGroupNameMap.put(groupName, metrics);

            } else {
                metricGroupNameMap.get(groupName).add(metric);
            }

        });


        return metricGroupNameMap;
    }


}
