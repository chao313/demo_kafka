package demo.kafka.config.redis;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServerBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 自定义的RedisServer
 */
public class LocalRedisServer extends LocalAbstractRedisInstance {
    private static final String REDIS_READY_PATTERN = ".*The server is now ready to accept connections on port.*";
    private static final int DEFAULT_REDIS_PORT = 6379;

    public LocalRedisServer() throws IOException {
        this(DEFAULT_REDIS_PORT);
    }

    public LocalRedisServer(Integer port) throws IOException {
        super(port);
        File executable = RedisExecProvider.defaultProvider().get();
        this.args = Arrays.asList(
                executable.getAbsolutePath(),
                "--port", Integer.toString(port)
        );
    }

    public LocalRedisServer(File executable, Integer port) {
        super(port);
        this.args = Arrays.asList(
                executable.getAbsolutePath(),
                "--port", Integer.toString(port)
        );
    }

    public LocalRedisServer(RedisExecProvider redisExecProvider, Integer port) throws IOException {
        super(port);
        this.args = Arrays.asList(
                redisExecProvider.get().getAbsolutePath(),
                "--port", Integer.toString(port)
        );
    }

    LocalRedisServer(List<String> args, int port) {
        super(port);
        this.args = new ArrayList<String>(args);
    }

    public static LocalRedisServerBuilder builder() {
        return new LocalRedisServerBuilder();
    }

    @Override
    protected String redisReadyPattern() {
        return REDIS_READY_PATTERN;
    }
}
