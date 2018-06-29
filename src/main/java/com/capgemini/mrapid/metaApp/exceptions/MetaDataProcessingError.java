package com.capgemini.mrapid.metaApp.exceptions;

/**
 * @author Anurag Udasi
 * custom exception for meta data processing error
 */

public class MetaDataProcessingError  extends Exception  {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public MetaDataProcessingError(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
