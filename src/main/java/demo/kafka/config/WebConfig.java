package demo.kafka.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.support.config.FastJsonConfig;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import demo.kafka.framework.Response;
import demo.kafka.util.InetAddressUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MVC拦截器
 */
@Slf4j
@Configuration
public class WebConfig implements WebMvcConfigurer {


    /**
     * 跨域支持
     *
     * @param registry
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowCredentials(true)
                .allowedMethods("GET", "POST", "DELETE", "PUT")
                .maxAge(3600 * 24);
    }


    /**
     * 配置消息转换器--这里我用的是alibaba 开源的 fastjson
     *
     * @param converters
     */
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        //1.需要定义一个convert转换消息的对象;
        FastJsonHttpMessageConverter fastJsonHttpMessageConverter = new FastJsonHttpMessageConverter();
        //2.添加fastJson的配置信息，比如：是否要格式化返回的json数据;
        FastJsonConfig fastJsonConfig = new FastJsonConfig();
        fastJsonConfig.setSerializerFeatures(SerializerFeature.PrettyFormat,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteNullStringAsEmpty,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.WriteNullListAsEmpty,
                SerializerFeature.WriteDateUseDateFormat);
        //3处理中文乱码问题
        List<MediaType> fastMediaTypes = new ArrayList<>();
        fastMediaTypes.add(MediaType.APPLICATION_JSON_UTF8);
        //4.在convert中添加配置信息.
        fastJsonHttpMessageConverter.setSupportedMediaTypes(fastMediaTypes);
        fastJsonHttpMessageConverter.setFastJsonConfig(fastJsonConfig);
        //5.将convert添加到converters当中.
        converters.add(fastJsonHttpMessageConverter);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HttpInterceptor()).addPathPatterns("/**");
    }

    /**
     * 自定义拦截器实现（拦截参数）
     */
    class HttpInterceptor implements HandlerInterceptor {

        String InterceptorParameterName = "bootstrap_servers";//拦截参数名称
        String InterceptorParameterRealName = "bootstrap.servers";//拦截参数名称

        /**
         * 在请求处理之前进行调用（Controller方法调用之前）
         */
        @Override
        public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
            //如果是SpringMVC请求
            if (handler instanceof HandlerMethod) {
                HandlerMethod handlerMethod = (HandlerMethod) handler;
//                log.info("当前拦截的方法为：{}", handlerMethod.getMethod().getName());
//                log.info("当前拦截的方法参数长度为：{}", handlerMethod.getMethod().getParameters().length);
//                log.info("当前拦截的方法为：{}", handlerMethod.getBean().getClass().getName());
//                System.out.println("开始拦截---------");
//                String uri = request.getRequestURI();
                for (MethodParameter methodParameter : handlerMethod.getMethodParameters()) {
                    if (methodParameter.getParameter().getName().equalsIgnoreCase(InterceptorParameterName)) {
                        try {
                            //如果是需要拦截的参数名称 10.202.16.136:9092
                            String value = request.getParameter(InterceptorParameterRealName);
                            if (!BootstrapServersConfig.getMapUseFul().containsValue(value)) {
                                /**
                                 * 如果不包含指定的ip的话，需要测试
                                 */
                                log.info("value:{}", value);
                                if (StringUtils.isNotBlank(value)) {
                                    String ip = value.substring(0, value.indexOf(":"));
                                    String port = value.substring(value.indexOf(":") + 1);
                                    boolean result = InetAddressUtil.isHostPortConnectable(ip, Integer.valueOf(port));
                                    if (result == false) {
                                        //如果ping不通
                                        response.setCharacterEncoding("UTF-8");
                                        response.getWriter().print(JSONObject.toJSONString(JSON.toJSON(Response.fail("指定ip指定端口无法连接:" + value))));
                                        return false;
                                    } else {
                                        /**
                                         * 加入
                                         */
                                        BootstrapServersConfig.getMapUseFul().put(value, value);
                                    }
                                } else {
                                    //如果ping不通
                                    response.setCharacterEncoding("UTF-8");
                                    response.getWriter().print(JSONObject.toJSONString(JSON.toJSON(Response.fail("传递的参数为null:"))));
                                    return false;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            return true;

        }

        /**
         * 请求处理之后进行调用，但是在视图被渲染之前（Controller方法调用之后），不一定会触发
         */
        @Override
        public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable ModelAndView modelAndView) throws Exception {
        }

        /**
         * 在整个请求结束之后被调用，也就是在DispatcherServlet 渲染了对应的视图之后执行
         * （主要是用于进行资源清理工作），肯定会触发
         */
        @Override
        public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        }

    }

}