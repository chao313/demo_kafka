package demo.kafka.controller.admin.service;

import demo.kafka.controller.admin.service.base.AdminService;
import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 作为Admin
 * -> 对于Topic需要有CRUD权限
 * getTopic
 * getTopics -> getTopicNames
 * create
 */
public class AdminTopicService extends AdminService {
    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     * {@link AdminFactory#getAdminTopicService(String)}
     */
    protected static AdminTopicService getInstance(String bootstrap_servers) {
        return new AdminTopicService(bootstrap_servers);
    }

    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminTopicService(String bootstrap_servers) {
        super(bootstrap_servers);
    }


    /**
     * 获取全部的 topics 原始状态的
     * 包含内部的topic
     * 比{@link #getTopicNames()}多了是否是内部的标识而已
     */
    public Collection<TopicListing> getTopicsResults() throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true); // 包含内部的 topic  - includes internal topics such as __consumer_offsets
        ListTopicsResult topics = super.client.listTopics(options);
        return topics.listings().get();
    }

    /**
     * 获取全部的 topics的名称
     */
    public Set<String> getTopicNames() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = super.client.listTopics();
        return listTopicsResult.names().get();
    }

    /**
     * 获取topic的描述
     */
    public TopicDescription getTopicDescription(String topic) throws ExecutionException, InterruptedException {
        Map<String, TopicDescription> topicToTopicDescriptionMap = this.describeTopic(Arrays.asList(topic));
        return topicToTopicDescriptionMap.get(topic);
    }

    /**
     * 获取topic的描述(集合)
     */
    public Collection<TopicDescription> getTopicDescription(Collection<String> topics) throws ExecutionException, InterruptedException {
        Map<String, TopicDescription> topicToTopicDescriptionMap = this.describeTopic(topics);
        return topicToTopicDescriptionMap.values();
    }


    /**
     * create multiple sample topics
     * 创建topic 这里会创建完成之后查询是否存topic
     *
     * @return <p></p>
     * true -> 创建成功
     * false -> 创建失败
     * @throws Exception 当目标topic已经存在时，会抛出异常
     */
    public boolean addTopic(String name, int numPartitions, short replicationFactor) throws Exception {
        if (this.getTopicNames().contains(name)) {
            throw new Exception("topic已经存在");
        }
        NewTopic newTopic = new NewTopic(name, numPartitions, replicationFactor);
        CreateTopicsResult createTopicsResult = super.client.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
        if (this.getTopicNames().contains(name)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * create multiple sample topics
     * 创建topic 这里会创建完成之后查询是否存topic
     * 这个是增强版
     *
     * @return <p></p>
     * true -> 创建成功
     * false -> 创建失败
     * @throws Exception 当目标topic已经存在时，会抛出异常
     */
    public boolean addTopic(String name, int numPartitions, short replicationFactor, Map<String, String> confgs) throws Exception {
        if (this.getTopicNames().contains(name)) {
            throw new Exception("topic已经存在");
        }
        NewTopic newTopic = new NewTopic(name, numPartitions, replicationFactor);
        newTopic.configs(confgs);//额外的配置
        CreateTopicsResult createTopicsResult = super.client.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
        if (this.getTopicNames().contains(name)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 删除topic 这里会创建完成之后查询是否存topic
     *
     * @return ：
     * true -> 创建成功
     * false -> 创建失败
     */
    public boolean deleteTopic(String name) throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = super.client.deleteTopics(Arrays.asList(name));
        deleteTopicsResult.all().get();
        if (!this.getTopicNames().contains(name)) {
            return true;
        } else {
            return false;
        }
    }


    /**
     * 判断 topic 是否存在
     */
    public boolean existTopicName(String name) throws ExecutionException, InterruptedException {
        return this.getTopicNames().contains(name);
    }

    /**
     * 描述topic
     */
    Map<String, TopicDescription> describeTopic(Collection<String> topics) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = super.client.describeTopics(topics);
        Map<String, TopicDescription> topicToTopicDescriptionMap = describeTopicsResult.all().get();
        return topicToTopicDescriptionMap;
    }


}
