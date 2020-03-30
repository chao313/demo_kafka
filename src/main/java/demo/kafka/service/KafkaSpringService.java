package demo.kafka.service;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

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

    @KafkaListener(topics = {"TP_01009411"})
    public void consumerFromKafkaServer(String msg) {
        //JSONObject jsonObject = JSONObject.parseObject(msg).getJSONObject("Data").getJSONObject("ContentData").getJSONObject("tzrw.html");
        this.sendToKafkaServer("TP_BDG_OSCORP_PERSON_BISSTRUCT", msg);
    }

    /**
     * 发送消息到kafka 指定 主题 和 msg
     */
    public void sendToKafkaServer(String topic, String msg) {
        kafkaTemplate.send(topic, msg);
    }


}