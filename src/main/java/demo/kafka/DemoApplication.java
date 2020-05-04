package demo.kafka;

import demo.kafka.config.redis.LocalRedisServer;
import org.apache.commons.io.IOUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import redis.embedded.RedisServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@SpringBootApplication
@EnableCaching
public class DemoApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        /**
         * 启动redis
         */
//        RedisServer redisServer = new RedisServer(6379);
        LocalRedisServer redisServer = LocalRedisServer.builder().port(6379).build();
        redisServer.start();
//        redisServer.start();
    }

}
