package demo.kafka.controller.response;

import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * {
 * title: {
 * text: '消息msg图'
 * * },
 * tooltip: {},
 * xAxis: {
 * data: ['衬衫', '羊毛衫', '雪纺衫', '裤子', '高跟鞋', '袜子']
 * },
 * yAxis: {},
 * series: [{
 * name: '消息msg图',
 * type: 'bar',
 * data: [5, 20, 36, 10, 10, 20]
 * }]
 */
@Data
public class EChartsVo {
    @Data
    public class Series {
        String name;
        String type;
        Collection<Integer> data;
    }

    @Data
    public class Title {
        String text;
    }

    @Data
    public class Tooltip {
    }

    @Data
    public class XAxis {
        Set<String> data;
    }

    @Data
    public class YAxis {
    }

    private Title title = new Title();
    private String text;
    private Tooltip tooltip = new Tooltip();
    private XAxis xAxis = new XAxis();
    private YAxis yAxis = new YAxis();
    private Series series = new Series();

    public static EChartsVo builder(String title, String seriesName, String seriesType) {
        EChartsVo eChartsVo = new EChartsVo();
        eChartsVo.title.text = title;
        eChartsVo.series.name = seriesName;
        eChartsVo.series.type = seriesType;
        return eChartsVo;
    }

    /**
     * 添加XAxisData
     *
     * @param data
     * @return
     */
    public EChartsVo addXAxisData(Set<String> data) {
        this.xAxis.data = data;
        return this;
    }

    /**
     * 添加seriesData
     */
    public EChartsVo addSeriesData(Collection<Integer> data) {
        this.series.data = data;
        return this;
    }

    public EChartsVo end() {
        return this;
    }

}


