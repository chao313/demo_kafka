package demo.kafka.controller.admin.service;

import org.apache.kafka.clients.admin.CreatePartitionsResult;
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
public class AdminPartitionsService extends AdminService {


    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     * {@link AdminFactory#getAdminPartitionsService(String)}
     */
    protected static AdminPartitionsService getInstance(String bootstrap_servers) {
        return new AdminPartitionsService(bootstrap_servers);
    }

    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminPartitionsService(String bootstrap_servers) {
        super(bootstrap_servers);
    }


    /**
     * 增加指定 topic 的 partition 的数量
     *
     * @throws ExecutionException : kakfa版本不支持操作: org.apache.kafka.common.errors.UnsupportedVersionException: The broker does not support CREATE_PARTITIONS
     * @throws ExecutionException : kafka的 partition的数量不支持减少 org.apache.kafka.common.errors.InvalidPartitionsException: Topic currently has 10 partitions, which is higher than the requested 1.
     * @throws ExecutionException : 要操作的topic不存在 org.apache.kafka.common.errors.UnknownTopicOrPartitionException: The topic 'TP_0100940511112' does not exist.
     */
    public boolean increasePartitions(String topic, int totalPartition) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(totalPartition);
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult createPartitionsResult = super.client.createPartitions(newPartitionsMap);
        createPartitionsResult.all().get();
        return true;
    }


}
