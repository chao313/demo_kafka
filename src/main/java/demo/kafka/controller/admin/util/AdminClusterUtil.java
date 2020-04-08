package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * 获取集群的信息
 */
public class AdminClusterUtil extends AdminUtil {


    /**
     * 获取实例
     */
    public static AdminClusterUtil getInstance(String bootstrap_servers) {
        return new AdminClusterUtil(bootstrap_servers);
    }


    /**
     * 构造函数(bootstrap_servers) 使用default来指定
     *
     * @param bootstrap_servers
     */
    AdminClusterUtil(String bootstrap_servers) {
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
