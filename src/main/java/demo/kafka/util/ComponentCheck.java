//package demo.kafka.util;
//
//import com.wind.count.config.SystemConfig;
//import com.wind.count.util.FileUtil;
//import kafka.admin.AdminUtils;
//import kafka.server.ConfigType;
//import kafka.utils.ZkUtils;
//import org.apache.kafka.common.security.JaasUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//
//public class ComponentCheck {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentCheck.class);
//
//
//
//    public static synchronized void topicChecker(Set<String> topics){
//        ZkUtils zkUtils = null;
//        try {
//            Properties kafkaProps = FileUtil.loadProperties("/kafka-consume.properties");
//            zkUtils = ZkUtils.apply(kafkaProps.getProperty("zookeeper.connect"), 300000, 300000, JaasUtils.isZkSecurityEnabled());
//
//            for(String topic:topics) {
//                Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
//                Iterator it = props.entrySet().iterator();
//                StringBuilder builder=new StringBuilder();
//                while (it.hasNext()) {
//                    Map.Entry entry = (Map.Entry) it.next();
//                    Object key = entry.getKey();
//                    Object value = entry.getValue();
//                    if ("retention.ms".equals(key)) {
//                        if (Long.valueOf(value.toString()) < 864000000L) {
//                            LOGGER.error(">{{service_type={},op_type=kafka-info,op_target={},info={}}", SystemConfig.getWindRunMode() + SystemConfig.getDb(), topic, key + "-" + value);
//                        }
//                    }
//                    builder.append(key+"-"+value+"_");
//                }
//                LOGGER.info(">{{service_type={},op_type=kafka-info,op_target={},info={}}", SystemConfig.getWindRunMode() + SystemConfig.getDb(), topic, builder.toString());
//
//            }
//        }catch (Exception e){
//            LOGGER.error(">{{service_type={},op_type=exception,info={}}}", SystemConfig.getWindRunMode() + SystemConfig.getDb(), e);
//        }finally {
//            try{
//                if(zkUtils!=null){
//                    zkUtils.close();
//                }
//            }catch (Exception e){
//                LOGGER.error(">{{service_type={},op_type=exception,info={}}}", SystemConfig.getWindRunMode() + SystemConfig.getDb(), e);
//            }
//        }
//    }
//}
