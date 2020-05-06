package demo.kafka;

import demo.kafka.config.redis.LocalRedisServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
@Slf4j
public class DemoApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Value("${redis.local.active}")
    private boolean redisLocalActive;

    @Value("${redis.local.maxheap}")
    private String maxheap;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        /**
         * 启动redis
         */
        if (redisLocalActive == true) {
            log.info("配置本地redis -> 启动失败");
//            LocalRedisServer redisServer = LocalRedisServer.builder().port(6379).build();
            LocalRedisServer redisServer = LocalRedisServer.builder().setting("maxheap " + maxheap).port(6379).build();
            redisServer.start();
            log.info("配置本地redis -> 启动成功");
        } else {
            log.info("配置本地redis -> 不启动，跳过");
        }
    }

}
