package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminPartitionsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminnPartitionsTest {

    private static AdminClient adminClient;


    @BeforeAll
    public static void BeforeAll() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "10.202.16.136:9092");
        adminClient = AdminClient.create(properties);
    }


    /**
     *
     */
    @Test
    public void createPartitions() throws ExecutionException, InterruptedException {
        boolean bool = AdminPartitionsUtil.increasePartitions(adminClient, "TP_0100940511112", 1);
        log.info("createPartitions:{}", bool);
    }


}
