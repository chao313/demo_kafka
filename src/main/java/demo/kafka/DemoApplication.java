package demo.kafka;

import demo.kafka.config.redis.LocalRedisServer;
import demo.kafka.config.redis.LocalRedisServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

    @Value("${redis.local.port}")
    private Integer port;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        /**
         * 启动redis
         */
        if (redisLocalActive == true) {
            log.info("配置本地redis -> 启动失败");
            LocalRedisServerBuilder builder = LocalRedisServer.builder().port(this.port);
            if (StringUtils.isNotBlank(maxheap)) {
                /**加入最大size*/
                builder.setting("maxheap " + maxheap).build();
            }
            LocalRedisServer redisServer = builder.build();
            redisServer.start();
            log.info("配置本地redis -> 启动成功");
        } else {
            log.info("配置本地redis -> 不启动，跳过");
        }
    }

}
