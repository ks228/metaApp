/**
 * FileFormatException
 * Custom file format check exception
 * @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.exceptions;

public class FileFormatException extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public FileFormatException(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
