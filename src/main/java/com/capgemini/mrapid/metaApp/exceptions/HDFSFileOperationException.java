/**
 * HDFSFileOperationException
 * HDFS fiel operation exception
 * @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.exceptions;


public class HDFSFileOperationException extends Exception {

	private static final long serialVersionUID = 1L;
	private String message = null;

	public HDFSFileOperationException(String message) {
		super();
		this.message = message;
	}
	
	 public String getMessage() {
	        return message;
	    }
}
