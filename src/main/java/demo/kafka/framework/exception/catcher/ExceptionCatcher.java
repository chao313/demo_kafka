package demo.kafka.framework.exception.catcher;


import demo.kafka.framework.Code;
import demo.kafka.framework.Response;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * 全局捕获异常
 */
@ControllerAdvice
public class ExceptionCatcher {

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public Response Exception(Exception e) {
        /**
         * 用于捕获全局异常，Controller发生异常，如果没有处理，就会在这里统一处理
         */
        Response response = new Response();
        response.setCode(Code.System.SYSTEM_ERROR_CODE);
        response.setMsg(Code.System.SYSTEM_ERROR_CODE_MSG);
        response.addException(e);
        response.setError(e.getMessage());
        return response;
    }
}
