/**
 * EmptyFileExcetion
 * Custom empty file exception
 * @author Anurag Udasi
 */
package com.capgemini.mrapid.metaApp.exceptions;

public class EmptyFileExcetion extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public EmptyFileExcetion(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
