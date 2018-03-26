package com.examples.entity;

import java.io.Serializable;

//must implement serializable interface
public class Ap implements  Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8181876944130733593L;
	
	

	private String apMac;
	 
	private String apName;

	public String getApMac() {
		return apMac;
	}

	public void setApMac(String apMac) {
		this.apMac = apMac;
	}

	public String getApName() {
		return apName;
	}

	public void setApName(String apName) {
		this.apName = apName;
	}
	 
	 
}
