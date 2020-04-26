package demo.kafka.controller.admin.service.base;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * topic相关
 */
public class AdminService {

    protected AdminClient client;

    private AdminService() {
    }

    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     */

    protected AdminService(String bootstrap_servers) {
        Properties properties = new Properties(); //新建一个Properties对象
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        AdminClient adminClient = AdminClient.create(properties);
        this.client = adminClient;
    }


}
