package com.capgemini.mrapid.metaApp.exceptions;

/**
 * @author Anurag Udasi
 * Custom exception for hive connection error
 *
 */
public class HiveConnectionExcetion extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public HiveConnectionExcetion(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
