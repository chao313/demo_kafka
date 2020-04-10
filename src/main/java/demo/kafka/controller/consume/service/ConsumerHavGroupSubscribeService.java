package demo.kafka.controller.consume.service;

import java.util.Collection;
import java.util.Set;

public class ConsumerHavGroupSubscribeService<K, V> extends ConsumerNoGroupService<K, V> {

    ConsumerHavGroupSubscribeService(KafkaConsumerService kafkaConsumerService, Collection<String> topics) {
        super(kafkaConsumerService);
        super.getKafkaConsumerService().subscribe(topics);
    }

    /**
     * 构造函数(直接注入 kafkaConsumer)
     */
    public static <K, V> ConsumerHavGroupSubscribeService<K, V> getInstance(KafkaConsumerService kafkaConsumerService, Collection<String> topics) {
        ConsumerHavGroupSubscribeService consumerHavAssignGroupService = new ConsumerHavGroupSubscribeService(kafkaConsumerService, topics);
        return consumerHavAssignGroupService;
    }


    /**
     * 查看订阅到的Partition
     */
    public Set<String> getPartitionSubscribed() {
        return this.getKafkaConsumerService().subscription();
    }


}
