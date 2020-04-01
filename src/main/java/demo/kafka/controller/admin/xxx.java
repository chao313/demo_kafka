package demo.kafka.controller.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class xxx {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "10.200.126.163:9092");
        AdminClient adminClient = AdminClient.create(properties);
        xxx.listAllTopics(adminClient);

//        xxx.createTopic("test11");

    }

    public static void createTopic(String topic) {

        try {
            AdminClient adminClient;
            Properties properties = new Properties();
            properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                    "10.200.126.163:9092");
            adminClient = AdminClient.create(properties);
            NewTopic newTopic = new NewTopic(topic, 2, (short) 2);
            adminClient.createTopics(Arrays.asList(newTopic));
//            adminClient.close();
            System.out.println("创建主题成功：" + topic);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * print all topics in the cluster
     *
     * @param client
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listAllTopics(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true); // 包含内部的 topic  - includes internal topics such as __consumer_offsets
        ListTopicsResult topics = client.listTopics(options);
        Set<String> topicNames = topics.names().get();//由于该类本质上是异步发送请求然后等待操作处理结果 ，必须要get来同步等待结果
        System.out.println("Current topics in this cluster: " + topicNames);
    }


}
