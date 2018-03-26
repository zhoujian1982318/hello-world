package com.examples.exceptions;

public class ApBizException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7197875197161514072L;

	private  String msg;
	
	public ApBizException() {
		super();
	}

	
	public ApBizException(Throwable cause) {
		super(cause);
	}
	
	public ApBizException(String msg) {
		super(msg);
		this.msg = msg;
	}


	public String getMsg() {
		return msg;
	}


	@Override
	public synchronized Throwable fillInStackTrace() {
		return this;
	}
	
	

}
