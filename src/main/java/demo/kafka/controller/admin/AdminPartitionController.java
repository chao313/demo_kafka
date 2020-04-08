package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.util.AdminClusterUtil;
import demo.kafka.controller.admin.util.AdminPartitionsUtil;
import demo.kafka.controller.response.DescribeClusterResultResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequestMapping(value = "/AdminPartitionController")
@RestController
public class AdminPartitionController {


    @ApiOperation(value = "增加topic的分区数量")
    @GetMapping(value = "/getCluster")
    public boolean getCluster(
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

