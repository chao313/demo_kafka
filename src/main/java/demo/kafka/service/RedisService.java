package demo.kafka.service;

import com.alibaba.fastjson.JSONObject;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class RedisService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 根据  scrollId 获取数据,设置有效期到5分钟后
     *
     * @param scrollId
     * @param pageNum
     * @param pageSize
     * @return
     */
    public Object getRecordByScrollId(
            String scrollId,
            Integer pageNum,
            Integer pageSize
    ) {
        PageInfo pageInfo = null;
        if (StringUtils.isNotBlank(scrollId)) {
            Long size = redisTemplate.opsForList().size(scrollId);
            Long end = Long.valueOf(pageNum * pageSize) - 1;
            if (pageNum * pageSize > size) {
                end = size;
            }
            List list = redisTemplate.opsForList().range(scrollId, (pageNum - 1) * pageSize, end);
            Page page = new Page(pageNum, pageSize, false);
            page.setTotal(size);
            page.setOrderBy(scrollId);
            page.addAll(list);
            pageInfo = new PageInfo(page);
            /**
             * 更新有效时间
             * 优化超时时间的设置 原来是 redisTemplate.opsForValue().set(uuid, changeResult, 5, TimeUnit.MINUTES);
             */
            redisTemplate.expire(scrollId, 5, TimeUnit.MINUTES);
        } else {
            throw new RuntimeException("scrollID已经失效,请点击查询按键，重新查询");
        }
        String JsonObject = new Gson().toJson(pageInfo);
        JSONObject result = JSONObject.parseObject(JsonObject);
        return result;
    }
}
