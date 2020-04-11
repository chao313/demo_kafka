package demo.kafka.framework.aspect;

import demo.kafka.framework.Code;
import demo.kafka.framework.Response;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 2018/8/9    Created by   chao
 */
@org.aspectj.lang.annotation.Aspect
@Component
public class ExecutionAspect {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 定义切面执行的方法
     */
    @Pointcut("execution(* demo.kafka.controller.*.*Controller.*(..))")
    private void pointCut() {
    }

    /**
     * ProceedingJoinPoint 继承的 JoinPoint 比JoinPoint ， 只多了执行的proceed Around </>一旦执行了这个方法，如果不调用proceed
     * ， 就会导致调用终止 注意：当核心业务抛异常后，立即退出，转向AfterAdvice 执行完AfterAdvice，再转到ThrowingAdvice
     */
    @Around(value = "pointCut()")
    public Object aroundAdvice(ProceedingJoinPoint joinPoint) throws Throwable {
        logger.info("第一步【执行Around：拦截方法的签名】joinPoint.getSignature() - > {}", joinPoint.getSignature());//Response demo.spring.boot.demospringboot.controller.pub.FrameworkController.framework(String,String)
        logger.info("第一步【执行Around：拦截目标的对象】joinPoint.getTarget() - > {}", joinPoint.getTarget());//demo.spring.boot.demospringboot.controller.pub.FrameworkController@25c6ab3f
        logger.info("第一步【执行Around：获取链接点的静态部分】joinPoint.getStaticPart() - > {}", joinPoint.getStaticPart());//execution(Response demo.spring.boot.demospringboot.controller.pub.FrameworkController.framework(String,String))
        logger.info("第一步【执行Around：拦截参数】joinPoint.getArgs() - > {}{}", joinPoint.getArgs());//王海潮123 | 注意这里要两个{}
        logger.info("第一步【执行Around：拦截切面的类型】joinPoint.getKind() - > {}", joinPoint.getKind());//method-execution || exception-handler
        logger.info("第一步【执行Around：拦截连接点方法所在类文件中的位置】joinPoint.getSourceLocation() - > {}", joinPoint.getSourceLocation());//org.springframework.aop.aspectj.MethodInvocationProceedingJoinPoint$SourceLocationImpl@6742526e
        logger.info("第一步【执行Around：拦截AOP的当前执行对象】joinPoint.getThis() - > {}", joinPoint.getThis());//demo.spring.boot.demospringboot.controller.pub.FrameworkController@25c6ab3

        Response response = new Response<>();
        try {
            response.setCode(Code.System.OK);
            response.setMsg(Code.System.SERVER_SUCCESS_MSG);
            Object result = joinPoint.proceed(); //继续下一个方法的调用 ：就是调用拦截的函数，得到拦截函数执行的结果
            logger.info("执行结果:{}", result);
            response.setContent(result);

        } catch (Exception e) {
            response.setCode(Code.System.FAIL);
            response.setMsg(e.toString());
            response.addException(e);
            logger.error("[getPicStationVoByLimitWeight ]FAIL path:{}", e.getMessage(), e);
        }
        return response;
        //这里的return的必须和拦截的方法的返回值一样
    }


//    /**
//     * Before
//     */
//    @Before(value = "pointCut()")
//    public void beforeAdvice(JoinPoint joinPoint) {
//        logger.info("第二步【执行Before：拦截方法的签名】joinPoint.getSignature() - > {}", joinPoint.getSignature());
//        logger.info("第二步【执行Before：拦截目标的对象】joinPoint.getTarget() - > {}", joinPoint.getTarget());
//        logger.info("第二步【执行Before：获取链接点的静态部分】joinPoint.getStaticPart() - > {}", joinPoint.getStaticPart());
//        logger.info("第二步【执行Before：拦截参数】joinPoint.getArgs() - > {}{}", joinPoint.getArgs());
//        logger.info("第二步【执行Before：拦截切面的类型】joinPoint.getKind() - > {}", joinPoint.getKind());
//        logger.info("第二步【执行Before：拦截连接点方法所在类文件中的位置】joinPoint.getSourceLocation() - > {}", joinPoint.getSourceLocation());
//        logger.info("第二步【执行Before：拦截AOP的当前执行对象】joinPoint.getThis() - > {}", joinPoint.getThis());
//    }
//
//
//    /**
//     * After 核心业务逻辑退出后（包括正常执行结束和异常退出），执行此Advice
//     */
//    @After(value = "pointCut()")
//    public void afterAdvice(JoinPoint joinPoint) {
//        logger.info("第三步【执行After：拦截方法的签名】joinPoint.getSignature() - > {}", joinPoint.getSignature());
//        logger.info("第三步【执行After：拦截目标的对象】joinPoint.getTarget() - > {}", joinPoint.getTarget());
//        logger.info("第三步【执行After：获取链接点的静态部分】joinPoint.getStaticPart() - > {}", joinPoint.getStaticPart());
//        logger.info("第三步【执行After：拦截参数】joinPoint.getArgs() - > {}{}", joinPoint.getArgs());
//        logger.info("第三步【执行After：拦截切面的类型】joinPoint.getKind() - > {}", joinPoint.getKind());
//        logger.info("第三步【执行After：拦截连接点方法所在类文件中的位置】joinPoint.getSourceLocation() - > {}", joinPoint.getSourceLocation());
//        logger.info("第三步【执行After：拦截AOP的当前执行对象】joinPoint.getThis() - > {}", joinPoint.getThis());
//    }
//
//    /**
//     * 正常return执行
//     * <p>
//     * 在执行代码return之后执行，没有发生异常，执行
//     */
//    @AfterReturning(value = "pointCut()", returning = "retVal")
//    public void afterReturningAdvice(JoinPoint joinPoint, Object retVal) {
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截方法的签名】joinPoint.getSignature() - > {}", joinPoint.getSignature());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截目标的对象】joinPoint.getTarget() - > {}", joinPoint.getTarget());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：获取链接点的静态部分】joinPoint.getStaticPart() - > {}", joinPoint.getStaticPart());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截参数】joinPoint.getArgs() - > {}{}", joinPoint.getArgs());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截切面的类型】joinPoint.getKind() - > {}", joinPoint.getKind());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截连接点方法所在类文件中的位置】joinPoint.getSourceLocation() - > {}", joinPoint.getSourceLocation());
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截AOP的当前执行对象】joinPoint.getThis() - > {}", joinPoint.getThis());
//
//
//        logger.info("第三步后-正常执行【正常return后AfterReturning：拦截return的值】joinPoint.getThis() - > {}", retVal);
//
////        logger.debug("=======>方法正常退出记录日志切面结束");
//    }
//
//    /**
//     * 异常后执行，处理错误信息
//     */
//    @AfterThrowing(value = "pointCut()", throwing = "ex")
//    public void afterThrowingAdvice(JoinPoint joinPoint, Exception ex) {
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：拦截方法的签名】joinPoint.getSignature() - > {}", joinPoint.getSignature());
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：拦截目标的对象】joinPoint.getTarget() - > {}", joinPoint.getTarget());
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：获取链接点的静态部分】joinPoint.getStaticPart() - > {}", joinPoint.getStaticPart());
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：拦截参数】joinPoint.getArgs() - > {}{}", joinPoint.getArgs());
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：拦截切面的类型】joinPoint.getKind() - > {}", joinPoint.getKind());
//        logger.info("第三步后-异常执行【异常return后AfterThrowing：拦截连接点方法所在类文件中的位置】joinPoint.getSourceLocation() - > {}", joinPoint.getSourceLocation());
//
//    }

}
