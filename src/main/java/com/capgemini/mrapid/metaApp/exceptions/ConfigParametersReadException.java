/**
 * ConfigParametersReadException
 * Custom property file read exception
 * @author Anurag Udasi
 */

package com.capgemini.mrapid.metaApp.exceptions;

public class ConfigParametersReadException extends Exception 
{	
	private static final long serialVersionUID = 1L;
	private String srcSystem;
	private String country;
	public ConfigParametersReadException(String srcSystem, String country)
	{	
		this.srcSystem = srcSystem;
		this.country = country;
		
	};
	
	public String get_srcSystem()
	{
		return srcSystem;
	}
	public String get_Country()
	{
		return country;
	}
}
