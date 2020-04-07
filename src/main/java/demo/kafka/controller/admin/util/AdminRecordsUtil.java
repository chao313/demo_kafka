package demo.kafka.controller.admin.util;

import demo.kafka.util.MapUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * 作为Admin,可以删除指定 offset之前的Record
 */
public class AdminRecordsUtil {

    /**
     * 底层的删除逻辑
     *
     * @param client
     * @param recordsToDeleteMap
     */
    public static void deleteRecords(AdminClient client, Map<TopicPartition, RecordsToDelete> recordsToDeleteMap) throws ExecutionException, InterruptedException {
        DeleteRecordsResult deleteRecordsResult = client.deleteRecords(recordsToDeleteMap);
        deleteRecordsResult.all().get();
    }


    /**
     * 调用删除逻辑
     * 这里只能删除指定 offset 之前的数据（偏移量是固定的，惟一的 -> 执行多次删除的效果一样）
     *
     * @param client
     */
    public static void deleteRecordsBeforeOffset(AdminClient client, TopicPartition topicPartition, RecordsToDelete recordsToDelete) throws ExecutionException, InterruptedException {
        AdminRecordsUtil.deleteRecords(client, MapUtil.$(topicPartition, recordsToDelete));
    }

}
