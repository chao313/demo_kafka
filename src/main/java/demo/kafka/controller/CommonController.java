package demo.kafka.controller;

import demo.kafka.config.BootstrapServersConfig;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RequestMapping(value = "/CommonController")
@RestController
public class CommonController {


    @Value("#{${bootstrap_servers}}")
    private Map<String, String> map;

    @ApiOperation(value = "根据 topic 获取分区信息", notes = "可以获取首领分区的节点,当前分区id,topic,同步的分区，完整的分区，离线的分区")
    @GetMapping(value = "/getKafkaBootstrapServers")
    public Object getKafkaBootstrapServers() {

        /**
         * 这里的地址是经过拦截的
         */
        return BootstrapServersConfig.getMapUseFul();
    }

}
