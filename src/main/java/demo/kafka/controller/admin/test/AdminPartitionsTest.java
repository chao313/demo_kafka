package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.util.AdminPartitionsUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminPartitionsTest {

    private static AdminPartitionsUtil adminPartitionsUtil = AdminPartitionsUtil.getInstance(Bootstrap.MY.getIp());

    /**
     *
     */
    @Test
    public void increasePartitions() throws ExecutionException, InterruptedException {
        boolean bool = adminPartitionsUtil.increasePartitions("TP_0100940511112", 1);
        log.info("increasePartitions:{}", bool);
    }


}
