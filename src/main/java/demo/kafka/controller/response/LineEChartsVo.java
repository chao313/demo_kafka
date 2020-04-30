package demo.kafka.controller.response;

import lombok.Data;

import java.util.Collection;
import java.util.Set;

/**
 * 基本折线图
 * option = {
 * tooltip: {    //提示框组件
 * <p>
 * trigger: 'axis'
 * <p>
 * },
 * xAxis: {
 * type: 'category',
 * data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
 * },
 * yAxis: {
 * type: 'value'
 * },
 * series: [{
 * data: [820, 932, 901, 934, 1290, 1330, 1320],
 * type: 'line'
 * }]
 * };
 */
@Data
public class LineEChartsVo {
    @Data
    public class XAxis {
        String type = "category";
        Set<String> data;
    }

    @Data
    public class Series {
        String type = "line";
        Collection<Long> data;
    }

    @Data
    public class YAxis {
        String type = "value";
    }

    @Data
    public class Tooltip {
        private String trigger = "axis";
    }


    private String text;
    private XAxis xAxis = new XAxis();
    private YAxis yAxis = new YAxis();
    private Series series = new Series();
    private Tooltip tooltip = new Tooltip();

    public static LineEChartsVo builder() {
        LineEChartsVo chartsVo = new LineEChartsVo();
        return chartsVo;
    }

    /**
     * 添加XAxisData
     *
     * @param data
     * @return
     */
    public LineEChartsVo addXAxisData(Set<String> data) {
        this.xAxis.data = data;
        return this;
    }

    /**
     * 添加seriesData
     */
    public LineEChartsVo addSeriesData(Collection<Long> data) {
        this.series.data = data;
        return this;
    }

    public LineEChartsVo end() {
        return this;
    }

}


