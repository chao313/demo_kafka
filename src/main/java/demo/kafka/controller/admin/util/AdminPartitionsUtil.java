package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewPartitions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * admin 分区操作
 * <p>
 * 可以指定分区在哪个 broker 上，这里暂时不做操作
 * Admin对Partition只有提高的功能
 */
public class AdminPartitionsUtil {

    /**
     * 增加指定 topic 的 partition 的数量
     *
     * @param client
     * @throws ExecutionException : kakfa版本不支持操作: org.apache.kafka.common.errors.UnsupportedVersionException: The broker does not support CREATE_PARTITIONS
     * @throws ExecutionException : kafka的 partition的数量不支持减少 org.apache.kafka.common.errors.InvalidPartitionsException: Topic currently has 10 partitions, which is higher than the requested 1.
     * @throws ExecutionException : 要操作的topic不存在 org.apache.kafka.common.errors.UnknownTopicOrPartitionException: The topic 'TP_0100940511112' does not exist.
     */
    public static boolean increasePartitions(AdminClient client, String topic, int totalPartition) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(totalPartition);
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult createPartitionsResult = client.createPartitions(newPartitionsMap);
        createPartitionsResult.all().get();
        return true;
    }


}
