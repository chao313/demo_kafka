package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 获取集群的信息
 */
public class AdminClusterUtil {

    /**
     * 获取集群的信息(可以获取节点的信息)
     * <p>
     * node -> value=[10.200.126.163:9092 (id: 0 rack: null)],exception=null,done=true
     *
     * @param client
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static DescribeClusterResult describeCluster(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeClusterResult describeClusterResult = client.describeCluster();
        describeClusterResult.nodes().get();
        return describeClusterResult;
    }


}
