package demo.kafka.service;

import com.alibaba.fastjson.JSONObject;
import demo.kafka.config.AwareUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;

@Slf4j
@Service
public class KafkaSpringService {


    @Resource
    private KafkaTemplate kafkaTemplate;

    /**
     * 监听 topic -> 把消息从一个topic上转到另一个topic
     *
     * @param msg
     */

    @KafkaListener(topics = {"Test11"})
    public void consumerFromKafkaServer(String msg) {
        //JSONObject jsonObject = JSONObject.parseObject(msg).getJSONObject("Data").getJSONObject("ContentData").getJSONObject("tzrw.html");
        log.info("获取:{}", msg);
//        this.sendToKafkaServer("TP_01009404", msg);
    }

    /**
     * 发送消息到kafka 指定 主题 和 msg
     */
    public void sendToKafkaServer(String topic, String msg) {
        kafkaTemplate.send(topic, msg);
    }

    //    @PostConstruct
    public void init() throws IOException {
        File file = AwareUtil.resourceLoader.getResource("classpath:琵琶行").getFile();
        FileUtils.readLines(file, "UTF-8").forEach(line -> {
            log.info("转发:{}", line);
            ListenableFuture listenableFuture = kafkaTemplate.send("Test11", line, line);
            listenableFuture.addCallback(new ListenableFutureCallback() {
                @Override
                public void onFailure(Throwable throwable) {
                    log.info("上传失败");
                }

                @Override
                public void onSuccess(Object o) {
                    log.info("上传成功");
                }
            });
        });
    }


}