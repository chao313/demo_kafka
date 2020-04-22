package demo.kafka.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
public class InetAddressUtil {
    /**
     * 是否可以ping通
     */
    public static boolean ping(String ipAddress, int timeOut) throws Exception {
        boolean status = InetAddress.getByName(ipAddress).isReachable(timeOut);
        // 当返回值是true时，说明host是可用的，false则不可。
        return status;
    }

    /**
     * 判断端口是否可用
     *
     * @param host
     * @param port
     * @return
     */
    public static boolean isHostPortConnectable(String host, int port) {
        Socket socket = new Socket();
        try {
            log.info("测试连接 {}:{} ...", host, port);
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            log.error("{}:{} -> 连接失败：{}", host, port, e.toString());
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("{}:{} -> 连接成功", host, port);
        return true;
    }

}
