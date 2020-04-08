package demo.kafka.controller.admin;


import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import demo.kafka.controller.admin.util.AdminClusterUtil;
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
@RequestMapping(value = "/AdminClusterController")
@RestController
public class AdminClusterController {


    @ApiOperation(value = "获取集群Cluster的信息")
    @GetMapping(value = "/getCluster")
    public JSONObject getCluster(
            @ApiParam(value = "kafka地址", allowableValues = "10.202.16.136:9092,192.168.0.105:9092,10.200.3.34:9092")
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers
    ) throws Exception {
        AdminClusterUtil adminClusterUtil = AdminClusterUtil.getInstance(bootstrap_servers);
        DescribeClusterResult describeClusterResult = adminClusterUtil.getCluster();
        DescribeClusterResultResponse describeClusterResultResponse = DescribeClusterResultResponse.addAll(describeClusterResult);
        String JsonObject = new Gson().toJson(describeClusterResultResponse);
        JSONObject result = JSONObject.parseObject(JsonObject);
        log.info("获取集群Cluster的信息:{}", result);
        return result;
    }


}
