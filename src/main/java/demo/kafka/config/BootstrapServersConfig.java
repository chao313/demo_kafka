package demo.kafka.config;

import demo.kafka.util.InetAddressUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 配置bootstrap的地址
 */
@Component
public class BootstrapServersConfig {

    /**
     * 存储有用的地址
     */
    public static Map<String, String> mapUseFul = new HashMap<>();//有用的


    @Value("#{${bootstrap_servers}}")
    private Map<String, String> map;

    @PostConstruct
    private void init() throws Exception {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String profile = entry.getKey();
            String ipAndPort = entry.getValue();
            String ip = ipAndPort.substring(0, ipAndPort.indexOf(":"));
            String port = ipAndPort.substring(ipAndPort.indexOf(":") + 1);
//                            boolean result = InetAddressUtil.result(ip, 200);

            if (InetAddressUtil.ping(ip, 100)) {
                if (InetAddressUtil.isHostPortConnectable(ip, Integer.valueOf(port))) {
                    /**
                     * 正常
                     */
                    mapUseFul.put(profile, ipAndPort);
                }
            }
        }
    }

    public static Map<String, String> getMapUseFul() {
        return mapUseFul;
    }
}

