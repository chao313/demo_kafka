package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminPartitionsService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminPartitionsTest {

    private static AdminPartitionsService adminPartitionsService = AdminPartitionsService.getInstance(Bootstrap.MY.getIp());

    /**
     *
     */
    @Test
    public void increasePartitions() throws ExecutionException, InterruptedException {
        boolean bool = adminPartitionsService.increasePartitions("TP_0100940511112", 1);
        log.info("increasePartitions:{}", bool);
    }


}
