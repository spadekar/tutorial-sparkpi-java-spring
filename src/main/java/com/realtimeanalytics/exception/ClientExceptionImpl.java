package com.realtimeanalytics.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
public class ClientExceptionImpl {
	
	@ExceptionHandler(value=ClientException.class)
	public ResponseEntity<ErrorDetail> clientError(HttpServletRequest request, Exception exception) {
	    ErrorDetail error = new ErrorDetail();
	    error.setStatus(HttpStatus.BAD_REQUEST.value());
	    error.setTitle("BAD_REQUEST");
	    error.setErrorMessage("Bad Request The server cannot process the request");
	   
	    return ResponseEntity.accepted().body(error);
	}

}
