package demo.kafka.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperUtil {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperUtil.class);
    private static final int ZOOKEEPER_TIMEOUT = 3000000;//zookeeper超时时间
    private static final CountDownLatch latch = new CountDownLatch(1);

    /**
     * 新建zookpeer的会话
     *
     * @param ipAndPort eg.  10.202.16.136:2181
     * @return
     */
    public static ZooKeeper getZookeeper(String ipAndPort) {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(ipAndPort, ZOOKEEPER_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zk;
    }

    /**
     * 获取zk的所有node
     *
     * @param fatherPath 上级路径，一般 "/"
     * @param paths
     * @param zk
     * @param result     -> 最终的返回结果 -> 包含了所有的node
     * @throws KeeperException
     * @throws InterruptedException
     */
    private static void getZkPath(String fatherPath, List<String> paths, ZooKeeper zk, List<String> result) throws KeeperException, InterruptedException {
        for (String path : paths) {
            logger.info("fatherPath:{} paths:{}", fatherPath, paths);
            String realPath = "";
            if (!fatherPath.equalsIgnoreCase("/")) {
                realPath = fatherPath + "/" + path;
            } else {
                realPath = fatherPath + path;
            }

            if (null != zk.exists(realPath, false)) {
                List<String> pathChildrens = zk.getChildren(realPath, false);
                result.add(realPath);
                logger.info("path:{}", realPath);
                if (pathChildrens.size() > 0) {
                    ZookeeperUtil.getZkPath(realPath, pathChildrens, zk, result);
                }
            }
        }
    }

    /**
     * 获取zk下的所有节点
     *
     * @param zk
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<String> getAllPath(ZooKeeper zk) throws KeeperException, InterruptedException {
        List<String> childrenRoot = zk.getChildren("/", false);
        List<String> result = new ArrayList<>();
        ZookeeperUtil.getZkPath("/", childrenRoot, zk, result);
        return result;
    }

    public static void main(String[] args) throws InterruptedException, KeeperException {
        String connectionString = "10.200.126.163:2181";
        ZooKeeper zookeeper = ZookeeperUtil.getZookeeper(connectionString);
        List<String> allPath = ZookeeperUtil.getAllPath(zookeeper);
        String path = "/config/topics/TP_01009406";
        byte[] data = zookeeper.getData(path, null, null);
        logger.info("path:{}", allPath);
        zookeeper.close();
    }
}

