package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminRecordsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminRecordTest {

    private AdminRecordsUtil adminRecordsUtil = AdminRecordsUtil.getInstance(Bootstrap.MY.getIp());


    /**
     *
     */
    @Test
    public void deleteRecordsBeforeOffset() throws ExecutionException, InterruptedException {
        adminRecordsUtil.deleteRecordsBeforeOffset(
                new TopicPartition("Test11", 0),
                RecordsToDelete.beforeOffset(10));
        log.info("record删除结束:{}");

    }

}
