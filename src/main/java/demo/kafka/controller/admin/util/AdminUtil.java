package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * topic相关
 */
public class AdminUtil {


    AdminClient client;

    private AdminUtil() {
    }

    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     */

    AdminUtil(String bootstrap_servers) {
        Properties properties = new Properties(); //新建一个Properties对象
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        AdminClient adminClient = AdminClient.create(properties);
        this.client = adminClient;
    }





}
