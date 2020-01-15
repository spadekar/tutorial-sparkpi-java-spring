package com.realtimeanalytics.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
public class DBExceptionImpl {
	
	@ExceptionHandler(value=DBException.class)
	public ResponseEntity<ErrorDetail> dbError(HttpServletRequest request, Exception exception) {
	    ErrorDetail error = new ErrorDetail();
	    error.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());	
	    error.setTitle("INTERNAL_SERVER_ERROR");
	    error.setErrorMessage("Service unavailable");
	    
	    return ResponseEntity.accepted().body(error);
	}

}
