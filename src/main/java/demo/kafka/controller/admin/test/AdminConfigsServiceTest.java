package demo.kafka.controller.admin.test;

import demo.kafka.controller.admin.service.AdminConfigsService;
import demo.kafka.controller.admin.service.AdminFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminConfigsServiceTest {

    AdminConfigsService adminConfigsService = AdminFactory.getAdminConfigsService(Bootstrap.MY.getIp());


    /**
     * 获取 配置 的信息（）
     */
    @Test
    public void describeConfigs() throws ExecutionException, InterruptedException, UnknownHostException {
        Map<ConfigResource, Config> configResourceConfigMap = adminConfigsService.getConfigs(ConfigResource.Type.TOPIC, "TP_010094051111");
        log.info("configResourceConfigMap:{}", configResourceConfigMap);
    }

    /**
     * 测试 获取 Topic 配置的信息
     * <p>
     * name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     */
    @Test
    public void describeTopicConfigs() throws ExecutionException, InterruptedException {
        Config config = adminConfigsService.getTopicConfigs("TP_010094051111");
        config.entries().forEach(configEntry -> {
            log.info("configEntry:{}", configEntry);
        });
    }


    /**
     * 测试 获取 Topic 配置的信息
     * <p>
     * name=advertised.host.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=metric.reporters, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=quota.producer.default, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.topic.num.partitions, value=50, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.flush.interval.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=controller.socket.timeout.ms, value=30000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=auto.create.topics.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.flush.interval.ms, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=principal.builder.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.socket.receive.buffer.bytes, value=65536, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.fetch.wait.max.ms, value=500, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=num.recovery.threads.per.data.dir, value=1, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.keystore.type, value=JKS, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=password.encoder.iterations, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.mechanism.inter.broker.protocol, value=GSSAPI, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=default.replication.factor, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.truststore.password, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=false, synonyms=[])
     * name=log.preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.kerberos.principal.to.local.rules, value=DEFAULT, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=fetch.purgatory.purge.interval.requests, value=1000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.endpoint.identification.algorithm, value=https, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.socket.timeout.ms, value=30000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=message.max.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=transactional.id.expiration.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.state.log.replication.factor, value=1, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=control.plane.listener.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=num.io.threads, value=8, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.login.refresh.buffer.seconds, value=300, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=connections.max.reauth.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=connection.failed.authentication.delay.ms, value=100, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.commit.required.acks, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.flush.offset.checkpoint.interval.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=delete.topic.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=quota.window.size.seconds, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.truststore.type, value=JKS, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=offsets.commit.timeout.ms, value=5000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=quota.window.num, value=11, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=zookeeper.connect, value=localhost:2182, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=authorizer.class.name, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=password.encoder.secret, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=true, synonyms=[])
     * name=log.cleaner.max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=num.replica.fetchers, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=alter.log.dirs.replication.quota.window.size.seconds, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.retention.ms, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=alter.log.dirs.replication.quota.window.num, value=11, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.roll.jitter.hours, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=password.encoder.old.secret, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=true, synonyms=[])
     * name=log.cleaner.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.load.buffer.size, value=5242880, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.client.auth, value=none, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=controlled.shutdown.max.retries, value=3, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.topic.replication.factor, value=1, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=queued.max.requests, value=500, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.state.log.min.isr, value=1, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.threads, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.secure.random.implementation, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.kerberos.service.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.kerberos.ticket.renew.jitter, value=0.05, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=socket.request.max.bytes, value=104857600, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.trustmanager.algorithm, value=PKIX, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=zookeeper.session.timeout.ms, value=6000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.jaas.config, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=false, synonyms=[])
     * name=log.message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.kerberos.min.time.before.relogin, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=zookeeper.set.acl, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=connections.max.idle.ms, value=600000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.retention.minutes, value=10080, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=max.connections, value=2147483647, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=delegation.token.expiry.time.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.state.log.num.partitions, value=50, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=replica.fetch.backoff.ms, value=1000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=inter.broker.protocol.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=kafka.metrics.reporters, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=listener.security.protocol.map, value=PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL, source=DEFAULT_CONFIG, isS
     * name=log.retention.hours, value=168, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=num.partitions, value=1, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=client.quota.callback.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=listeners, value=PLAINTEXT://10.202.16.136:9092, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=broker.id.generation.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.provider, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.enabled.protocols, value=TLSv1.2,TLSv1.1,TLSv1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=inter.broker.listener.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=delete.records.purgatory.purge.interval.requests, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.roll.ms, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=alter.config.policy.class.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=delegation.token.expiry.check.interval.ms, value=3600000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.cipher.suites, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=zookeeper.max.in.flight.requests, value=10, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.flush.scheduler.interval.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.index.size.max.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.keymanager.algorithm, value=SunX509, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.login.callback.handler.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=security.inter.broker.protocol, value=PLAINTEXT, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=replica.fetch.max.bytes, value=1048576, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.server.callback.handler.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=advertised.port, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.dedupe.buffer.size, value=134217728, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.high.watermark.checkpoint.interval.ms, value=5000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=replication.quota.window.size.seconds, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.io.buffer.size, value=524288, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.kerberos.ticket.renew.window.factor, value=0.8, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=create.topic.policy.class.name, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=zookeeper.connection.timeout.ms, value=6000, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=metrics.recording.level, value=INFO, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=password.encoder.cipher.algorithm, value=AES/CBC/PKCS5Padding, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=controlled.shutdown.retry.backoff.ms, value=5000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=security.providers, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.roll.hours, value=168, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.flush.start.offset.checkpoint.interval.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.principal.mapping.rules, value=DEFAULT, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=host.name, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=replica.selector.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.roll.jitter.ms, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=transaction.state.log.segment.bytes, value=104857600, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=max.connections.per.ip, value=2147483647, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=offsets.topic.segment.bytes, value=104857600, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=background.threads, value=10, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=quota.consumer.default, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=request.timeout.ms, value=30000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=group.initial.rebalance.delay.ms, value=0, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.login.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.dir, value=/tmp/kafka-logs, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.cleaner.backoff.ms, value=15000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=offset.metadata.max.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.truststore.location, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.fetch.response.max.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=group.max.session.timeout.ms, value=1800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.keystore.password, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=false, synonyms=[])
     * name=port, value=9092, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=zookeeper.sync.time.ms, value=2000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.retention.minutes, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.segment.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.dirs, value=/wind/tmp/kafka-logs, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=controlled.shutdown.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=max.connections.per.ip.overrides, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=sasl.login.refresh.min.period.seconds, value=60, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=password.encoder.key.length, value=128, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.login.refresh.window.factor, value=0.8, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=kafka.metrics.polling.interval.secs, value=10, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.abort.timed.out.transaction.cleanup.interval.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms
     * name=sasl.kerberos.kinit.cmd, value=/usr/bin/kinit, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=log.cleaner.io.max.bytes.per.second, value=1.7976931348623157E308, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=auto.leader.rebalance.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=leader.imbalance.check.interval.seconds, value=300, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.min.cleanable.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.lag.time.max.ms, value=10000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=max.incremental.fetch.session.cache.slots, value=1000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=delegation.token.master.key, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=true, synonyms=[])
     * name=num.network.threads, value=3, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.key.password, value=null, source=DEFAULT_CONFIG, isSensitive=true, isReadOnly=false, synonyms=[])
     * name=reserved.broker.max.id, value=1000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.client.callback.handler.class, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=metrics.num.samples, value=2, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.remove.expired.transaction.cleanup.interval.ms, value=3600000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonym
     * name=socket.send.buffer.bytes, value=102400, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=ssl.protocol, value=TLS, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=password.encoder.keyfactory.algorithm, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=transaction.state.log.load.buffer.size, value=5242880, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=socket.receive.buffer.bytes, value=102400, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=ssl.keystore.location, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=replica.fetch.min.bytes, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=broker.rack, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=num.replica.alter.log.dirs.threads, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.enabled.mechanisms, value=GSSAPI, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=group.min.session.timeout.ms, value=6000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.retention.check.interval.ms, value=600000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.cleaner.io.buffer.load.factor, value=0.9, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=transaction.max.timeout.ms, value=900000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=producer.purgatory.purge.interval.requests, value=1000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=metrics.sample.window.ms, value=30000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=group.max.size, value=2147483647, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=broker.id, value=0, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=offsets.topic.compression.codec, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=delegation.token.max.lifetime.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=replication.quota.window.num, value=11, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=log.retention.check.interval.ms, value=300000, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=advertised.listeners, value=null, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=leader.imbalance.per.broker.percentage, value=10, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     * name=sasl.login.refresh.window.jitter, value=0.05, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     * name=queued.max.request.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=true, synonyms=[])
     */
    @Test
    public void describeBrokerConfigs() throws ExecutionException, InterruptedException {
        Config config = adminConfigsService.getBrokerConfigs(0);
        config.entries().forEach(configEntry -> {
            log.info("configEntry:{}", configEntry);
        });
    }


    @Test
    public void incrementalAlterTopicConfigs() throws ExecutionException, InterruptedException {

        ConfigEntry configEntry = new ConfigEntry("unclean.leader.election.enable", "false");

        adminConfigsService.updateTopicConfigs("Test11", Arrays.asList(configEntry), AlterConfigOp.OpType.SUBTRACT);

    }

    /**
     * 确实可以修改
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void incrementalAlterBrokerConfigs() throws ExecutionException, InterruptedException {

        ConfigEntry configEntry = new ConfigEntry("listeners", "PLAINTEXT://10.202.16.136:9092");

        adminConfigsService.updateBrokerConfigs(0, Arrays.asList(configEntry), AlterConfigOp.OpType.SET);

    }

}
