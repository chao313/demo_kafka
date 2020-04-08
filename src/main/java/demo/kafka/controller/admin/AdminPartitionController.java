package demo.kafka.controller.admin;


import demo.kafka.controller.admin.util.AdminPartitionsUtil;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequestMapping(value = "/AdminPartitionController")
@RestController
public class AdminPartitionController {


    @ApiOperation(value = "增加topic的分区数量")
    @GetMapping(value = "/increasePartitions")
    public boolean increasePartitions(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @ApiParam(value = "topic-name")
            @RequestParam(name = "topic", defaultValue = "Test")
                    String topic,
            @ApiParam(value = "totalPartition")
            @RequestParam(name = "totalPartition", defaultValue = "2")
                    int totalPartition
    ) throws Exception {
        AdminPartitionsUtil adminPartitionsUtil = AdminPartitionsUtil.getInstance(bootstrap_servers);
        boolean isSuccess = adminPartitionsUtil.increasePartitions(topic, totalPartition);
        log.info("increasePartitions:{}", isSuccess);
        return isSuccess;
    }


}

