package demo.kafka.controller.wind;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.File;
import java.io.IOException;
import java.util.Date;

@Slf4j
public class ProducerUpload {

    private static FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");


    public static KafkaMsgRequest generateKafkaRequestMsg(
            String policyID,
            String topic,
            String fileName,
            byte[] fileBytes
    ) {
        KafkaMsgRequest msgRequest = new KafkaMsgRequest();
        msgRequest.msgID = fileName;
        msgRequest.msgHead.setEncType("ORI_FILE");
        msgRequest.msgHead.setTopic(topic);
        msgRequest.msgHead.setUpdateTime(fastDateFormat.format(new Date()));
        msgRequest.msgData.setFileName(fileName);
        msgRequest.msgData.setOrgUrl("");
        msgRequest.msgData.setPolicyId(policyID);
        String data = Base64.encodeBase64String(fileBytes);
        msgRequest.msgData.setData(data);
        log.info("生成结束:");
        return msgRequest;
    }


    public static void main(String[] args) throws InterruptedException {
        System.out.println(null == null);


        String policyID = null;
        String topic = null;
        int depth = 0;
        String foderPath = "";
        long time = 0L;
        int beginIndex = 0;

        args = new String[]{"OCMSZS", "OCMSZS", "1", "D:\\wind\\WCBData\\tempCompUpdFile\\PEVC_开放马萨诸塞州和俄亥俄州\\OCMSZS", "5000", "0"};


//        if (args.length == 6) {
//            policyID = args[0];
//            topic = args[1];
//            depth = Integer.valueOf(args[2]);
//            foderPath = args[3];
//            time = Long.valueOf(args[4]);
//            beginIndex = Integer.valueOf(args[5]);
//            log.info("策略名： {}", policyID);
//            log.info("topic： {}", topic);
//            log.info("文件夹深度： {}", depth);
//            log.info("上传文件所在路径： {}", foderPath);
//            log.info("每次上传间隔时间(ms)： {}", time);
//            log.info("本次上传起点： {}", beginIndex);
//            generateKafkaRequestMsg(policyID, topic);
//
//
//        }

    }
}
