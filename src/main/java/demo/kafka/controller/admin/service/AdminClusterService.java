package demo.kafka.controller.admin.service;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * 获取集群的信息
 */
public class AdminClusterService extends AdminService {


    /**
     * 获取实例 ( 不对外开放，由工厂来获取 )
     * {@link AdminFactory#getAdminClusterService(String)}}
     */
    protected static AdminClusterService getInstance(String bootstrap_servers) {
        return new AdminClusterService(bootstrap_servers);
    }


    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminClusterService(String bootstrap_servers) {
        super(bootstrap_servers);
    }

    /**
     * 获取集群的信息(可以获取节点的信息)
     * <p>
     * node -> value=[10.200.126.163:9092 (id: 0 rack: null)],exception=null,done=true
     *
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public DescribeClusterResult getCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = super.client.describeCluster();
        describeClusterResult.nodes().get();
        return describeClusterResult;
    }

    /**
     * 获取集群中的 Broker信息
     */
    public Collection<Node> getBrokersInCluster() throws ExecutionException, InterruptedException {
        return this.getCluster().nodes().get();
    }


}
