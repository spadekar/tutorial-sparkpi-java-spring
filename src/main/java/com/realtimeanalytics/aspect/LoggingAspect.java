package com.realtimeanalytics.aspect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.context.annotation.Configuration;

@Aspect
@Configuration
public class LoggingAspect {
	
private static final Logger logger = LogManager.getLogger(LoggingAspect.class);
	
	@Before("execution(* com.realtimeanalytics.controller.*.*(..)) || execution(* com.realtimeanalytics.security.*.*(..)) || execution(* com.realtimeanalytics.service.*.*(..))")
	public void logBefore(JoinPoint joinPoint) {
		logger.info("Starting: " + joinPoint.getTarget().getClass()+ "." + joinPoint.getSignature().getName() + "()");
	}
	
	@After("execution(* com.realtimeanalytics.controller.*.*(..)) || execution(* com.realtimeanalytics.security.*.*(..)) || execution(* com.realtimeanalytics.service.*.*(..))")
	public void logAfter(JoinPoint joinPoint) {
		logger.info("Ending: " + joinPoint.getTarget().getClass()+ "." + joinPoint.getSignature().getName() + "()");

	}
	
	@AfterReturning(
			pointcut = "execution(* com.realtimeanalytics.controller.*.*(..)) || execution(* com.realtimeanalytics.security.*.*(..)) || execution(* com.realtimeanalytics.service.*.*(..))",
			returning= "result")
	public void logAfterReturning(JoinPoint joinPoint, Object result) {
		logger.info("Returning: " + joinPoint.getTarget().getClass()+ "." + joinPoint.getSignature().getName() + "()");
		logger.debug("Result: "+ result);
	}
	
	 @AfterThrowing(
		      pointcut = "execution(* com.realtimeanalytics.controller.*.*(..)) || execution(* com.realtimeanalytics.security.*.*(..)) || execution(* com.realtimeanalytics.service.*.*(..))",
		      throwing= "error")
	 public void logAfterThrowing(JoinPoint joinPoint, Throwable error) {
		 logger.info("Thrwoing Exception: " + joinPoint.getTarget().getClass()+ "." + joinPoint.getSignature().getName() + "()");
		 logger.error("Exception: "+ error);
	 }

}
