package demo.kafka.controller.stream.test;

import demo.kafka.controller.admin.test.Bootstrap;
import demo.kafka.controller.stream.KStreamFilter;
import demo.kafka.controller.stream.PropertiesFactoryStream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

public class KStreamFilterTest extends KStreamBase {

    /**
     * 流构造器
     */
    protected static StreamsBuilder builder = new StreamsBuilder();
    ;

    /**
     * 测试过滤符合要求的value的值，转发到另一个topic
     *
     * @throws InterruptedException
     */
    @Test
    public void filterMatch() throws InterruptedException {

        KStreamFilter kStreamFilter = new KStreamFilter();
        kStreamFilter.filterMatch(
                Arrays.asList("Test"),
                Pattern.compile("\\d{3}"),
                Pattern.compile(".*"),
                "output", 5000L,
                PropertiesFactoryStream.create("id", Bootstrap.HONE.getIp())
        );
    }

    /**
     * 测试过滤符合要求的value的值，转发到另一个topic
     *
     * @throws InterruptedException
     */
    @Test
    public void filterContain() throws InterruptedException {

        KStreamFilter kStreamFilter = new KStreamFilter();
        kStreamFilter.filterContain(
                Arrays.asList("Test"),
                null,
                null,
                "output", 5000L,
                PropertiesFactoryStream.create("id", Bootstrap.HONE.getIp())
        );
    }

    @Test
    public void xx() {
        Pattern.compile("\\d*").matcher("11222.222.xx");
    }

}
