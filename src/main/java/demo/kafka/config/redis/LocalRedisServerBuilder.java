package demo.kafka.config.redis;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import redis.embedded.RedisExecProvider;
import redis.embedded.exceptions.RedisBuildingException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义的RedisBuilder
 */
public class LocalRedisServerBuilder {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String CONF_FILENAME = "embedded-redis-server";

    private File executable;
    private RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider();
    private Integer port = 6379;
    private InetSocketAddress slaveOf;
    private String redisConf;

    private StringBuilder redisConfigBuilder;

    public LocalRedisServerBuilder redisExecProvider(RedisExecProvider redisExecProvider) {
        this.redisExecProvider = redisExecProvider;
        return this;
    }

    public LocalRedisServerBuilder port(Integer port) {
        this.port = port;
        return this;
    }

    public LocalRedisServerBuilder slaveOf(String hostname, Integer port) {
        this.slaveOf = new InetSocketAddress(hostname, port);
        return this;
    }

    public LocalRedisServerBuilder slaveOf(InetSocketAddress slaveOf) {
        this.slaveOf = slaveOf;
        return this;
    }

    public LocalRedisServerBuilder configFile(String redisConf) {
        if (redisConfigBuilder != null) {
            throw new RedisBuildingException("Redis configuration is already partially build using setting(String) method!");
        }
        this.redisConf = redisConf;
        return this;
    }

    public LocalRedisServerBuilder setting(String configLine) {
        if (redisConf != null) {
            throw new RedisBuildingException("Redis configuration is already set using redis conf file!");
        }

        if (redisConfigBuilder == null) {
            redisConfigBuilder = new StringBuilder();
        }

        redisConfigBuilder.append(configLine);
        redisConfigBuilder.append(LINE_SEPARATOR);
        return this;
    }

    public LocalRedisServer build() {
        tryResolveConfAndExec();
        List<String> args = buildCommandArgs();
        return new LocalRedisServer(args, port);
    }

    public void reset() {
        this.executable = null;
        this.redisConfigBuilder = null;
        this.slaveOf = null;
        this.redisConf = null;
    }

    private void tryResolveConfAndExec() {
        try {
            resolveConfAndExec();
        } catch (IOException e) {
            throw new RedisBuildingException("Could not build server instance", e);
        }
    }

    private void resolveConfAndExec() throws IOException {
        if (redisConf == null && redisConfigBuilder != null) {
            File redisConfigFile = File.createTempFile(resolveConfigName(), ".conf");
            redisConfigFile.deleteOnExit();
            Files.write(redisConfigBuilder.toString(), redisConfigFile, Charset.forName("UTF-8"));
            redisConf = redisConfigFile.getAbsolutePath();
        }

        try {
            executable = redisExecProvider.get();
        } catch (Exception e) {
            throw new RedisBuildingException("Failed to resolve executable", e);
        }
    }

    private String resolveConfigName() {
        return CONF_FILENAME + "_" + port;
    }

    private List<String> buildCommandArgs() {
        List<String> args = new ArrayList<String>();
        args.add(executable.getAbsolutePath());

        if (!Strings.isNullOrEmpty(redisConf)) {
            args.add(redisConf);
        }

        if (port != null) {
            args.add("--port");
            args.add(Integer.toString(port));
        }

        if (slaveOf != null) {
            args.add("--slaveof");
            args.add(slaveOf.getHostName());
            args.add(Integer.toString(slaveOf.getPort()));
        }

        return args;
    }
}
