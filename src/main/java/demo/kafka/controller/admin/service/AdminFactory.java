package demo.kafka.controller.admin.service;

/**
 * 专门生产Admin
 */
public class AdminFactory {

    /**
     * 生产 AdminClusterService 相关的
     */
    public static AdminClusterService getAdminClusterService(String bootstrap_servers) {
        return AdminClusterService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminConfigsService 相关的
     */
    public static AdminConfigsService getAdminConfigsService(String bootstrap_servers) {
        return AdminConfigsService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminConsumerGroupsService 相关的
     */
    public static AdminConsumerGroupsService getAdminConsumerGroupsService(String bootstrap_servers) {
        return AdminConsumerGroupsService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminMetricService 相关的
     */
    public static AdminMetricService getAdminMetricService(String bootstrap_servers) {
        return AdminMetricService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminPartitionsService 相关的
     */
    public static AdminPartitionsService getAdminPartitionsService(String bootstrap_servers) {
        return AdminPartitionsService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminRecordsService 相关的
     */
    public static AdminRecordsService getAdminRecordsService(String bootstrap_servers) {
        return AdminRecordsService.getInstance(bootstrap_servers);
    }

    /**
     * 生产 AdminTopic 相关的
     */
    public static AdminTopicService getAdminTopicService(String bootstrap_servers) {
        return AdminTopicService.getInstance(bootstrap_servers);
    }

}
