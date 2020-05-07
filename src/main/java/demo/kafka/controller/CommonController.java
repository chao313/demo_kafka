package demo.kafka.controller;

import demo.kafka.config.BootstrapServersConfig;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RequestMapping(value = "/CommonController")
@RestController
public class CommonController {

    @Value("#{${all_bootstrap_servers}}")
    private Map<String, String> map;

    @ApiOperation(value = "获取指定的kafka地址")
    @GetMapping(value = "/getKafkaBootstrapServers")
    public Object getKafkaBootstrapServers() {

        /**
         * 这里的地址是经过拦截的
         */
        return BootstrapServersConfig.getMapUseFul();
    }

    @ApiOperation(value = "获取全部的kafka地址")
    @GetMapping(value = "/getAllKafkaBootstrapServers")
    public Object getAllKafkaBootstrapServers() {

        /**
         * 这里的地址是全部的
         */
        return map;
    }

}
