package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminRecordsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j

public class AdminRecordTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Bootstrap.HONE.getIp());
        adminClient = AdminClient.create(properties);
    }


    /**
     */
    @Test
    public void deleteRecordsBeforeOffset() throws ExecutionException, InterruptedException {
        AdminRecordsUtil.deleteRecordsBeforeOffset(
                adminClient,
                new TopicPartition("Test11", 0),
                RecordsToDelete.beforeOffset(10));
        log.info("record删除结束:{}");

    }


}
