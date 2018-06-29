package com.capgemini.mrapid.metaApp.exceptions;

/**
 * @author Anurag Udasi
 * Custom exception for property value missing
 */
public class PropertyValueNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public PropertyValueNotFoundException(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
