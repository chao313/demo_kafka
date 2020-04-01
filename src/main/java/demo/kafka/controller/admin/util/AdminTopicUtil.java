package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class AdminTopicUtil {
    
    /**
     * 获取全部的 topics 原始状态的
     * 包含内部的topic
     *
     * @param client
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static ListTopicsResult listTopics(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true); // 包含内部的 topic  - includes internal topics such as __consumer_offsets
        ListTopicsResult topics = client.listTopics(options);
        topics.names().get();
        return topics;
    }

    /**
     * 获取全部的 topics的名称
     *
     * @param client
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static Set<String> listTopicNames(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = AdminTopicUtil.listTopics(client);
        return listTopicsResult.names().get();
    }

    /**
     * 判断 topic 是否存在
     */
    public static boolean existTopicName(AdminClient client, String name) throws ExecutionException, InterruptedException {
        return AdminTopicUtil.listTopicNames(client).contains(name);
    }

    /**
     * create multiple sample topics
     * 创建topic 这里会创建完成之后查询是否存topic
     *
     * @param client
     * @return <p></p>
     * true -> 创建成果
     * false -> 创建失败
     * @throws Exception 当目标topic已经存在时，会抛出异常
     */
    public static boolean createTopic(AdminClient client, String name, int numPartitions, short replicationFactor) throws Exception {
        if (AdminTopicUtil.listTopicNames(client).contains(name)) {
            throw new Exception("topic已经存在");
        }
        NewTopic newTopic = new NewTopic(name, numPartitions, replicationFactor);
        CreateTopicsResult createTopicsResult = client.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
        if (AdminTopicUtil.listTopicNames(client).contains(name)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 删除topic 这里会创建完成之后查询是否存topic
     *
     * @return <p></p>
     * true -> 创建成果
     * false -> 创建失败
     */
    public static boolean deleteTopics(AdminClient client, String name) throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(name));
        deleteTopicsResult.all().get();
        if (!AdminTopicUtil.listTopicNames(client).contains(name)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 描述topic
     */
    public static DescribeTopicsResult describeTopic(AdminClient client, String name) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(name));
        describeTopicsResult.all().get();
        return describeTopicsResult;
    }


}
