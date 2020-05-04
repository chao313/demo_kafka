package demo.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import redis.embedded.RedisServer;

import java.io.IOException;

//@SpringBootTest
class DemoApplicationTests {

    @Test
    void contextLoads() throws IOException {
        /**
         * 启动redis
         */

    }

    public static void main(String[] args) {
        RedisServer redisServer = RedisServer.builder()
                .port(6379)
                .setting("maxmemory 128")
                .build();
        redisServer.start();
    }
}
