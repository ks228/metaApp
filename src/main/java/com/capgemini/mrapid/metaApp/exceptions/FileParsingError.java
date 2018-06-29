/**
 * FileParsingError
 * Custome file parsing error exception
 * @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.exceptions;

public class FileParsingError extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public FileParsingError(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
