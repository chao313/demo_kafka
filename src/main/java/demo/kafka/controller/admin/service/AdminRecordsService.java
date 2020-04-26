package demo.kafka.controller.admin.service;

import demo.kafka.controller.admin.service.base.AdminService;
import demo.kafka.util.MapUtil;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 作为Admin,可以删除指定 offset之前的Record
 * 对 Record 只有删除的功能
 */
public class AdminRecordsService extends AdminService {

    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     * {@link AdminFactory#getAdminRecordsService(String)}
     */
    protected static AdminRecordsService getInstance(String bootstrap_servers) {
        return new AdminRecordsService(bootstrap_servers);
    }


    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminRecordsService(String bootstrap_servers) {
        super(bootstrap_servers);
    }

    /**
     * 底层的删除逻辑
     *
     * @param recordsToDeleteMap
     */
    public void deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDeleteMap) throws ExecutionException, InterruptedException {
        DeleteRecordsResult deleteRecordsResult = super.client.deleteRecords(recordsToDeleteMap);
        deleteRecordsResult.all().get();
    }


    /**
     * 调用删除逻辑
     * 这里只能删除指定 offset 之前的数据（偏移量是固定的，惟一的 -> 执行多次删除的效果一样）
     */
    public void deleteRecordsBeforeOffset(TopicPartition topicPartition, RecordsToDelete recordsToDelete) throws ExecutionException, InterruptedException {
        this.deleteRecords(MapUtil.$(topicPartition, recordsToDelete));
    }

}
