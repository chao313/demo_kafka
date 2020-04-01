package demo.kafka.controller.admin.util;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class AdminClusterUtil {

    /**
     * 查询集群的相关信息
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
