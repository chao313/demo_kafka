package demo.kafka.controller.consume;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.consume.service.KafkaOffsetService;
import demo.kafka.controller.response.OffsetRecordResponse;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;


/**
 * 操作offset
 */
@Slf4j
@RequestMapping(value = "/OffsetController")
@RestController
public class OffsetController {


    @ApiOperation(value = "获取指定group的最新的groupMeta的offset记录")
    @GetMapping(value = "/getLastGroupMetadataOffsetRecord")
    public Object getLastGroupMetadataOffsetRecord(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group.id")
                    String group_id
    ) {
        OffsetRecordResponse lastGroupMetadataOffsetRecord
                = KafkaOffsetService.getLastGroupMetadataOffsetRecord(bootstrap_servers, group_id);
        return lastGroupMetadataOffsetRecord;
    }


    @ApiOperation(value = "获取指定group订阅的topic")
    @GetMapping(value = "/getSubscribedTopics")
    public Object getSubscribedTopics(
            @ApiParam(value = "kafka", allowableValues = Bootstrap.allowableValues)
            @RequestParam(name = "bootstrap.servers", defaultValue = "10.202.16.136:9092")
                    String bootstrap_servers,
            @RequestParam(name = "group.id")
                    String group_id
    ) {

        Set<String> subscribedTopics
                = KafkaOffsetService.getSubscribedTopics(bootstrap_servers, group_id);
        return subscribedTopics;
    }


}
