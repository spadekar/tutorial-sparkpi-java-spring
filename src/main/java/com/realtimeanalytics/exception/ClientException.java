package com.realtimeanalytics.exception;

public class ClientException extends RuntimeException{
	
	private static final long serialVersionUID = 100L;

	public ClientException(Exception e) {
		super(e);
	}

	public ClientException() {
		
	}

}
