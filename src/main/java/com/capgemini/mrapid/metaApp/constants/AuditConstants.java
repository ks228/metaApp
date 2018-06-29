/**
 * AuditConstants
 * Contains all audit logger required constants 
 * @author Anuradha Dede
 */
package com.capgemini.mrapid.metaApp.constants;

public class AuditConstants {
	public static String ERRORMSG = "ERROR"; 
	public static String SUCCESS = "SUCCESS"; 
	public static String ERROR = "ERROR"; 
	public static String NOERROR = "No error"; 
	public static String AUDIT_ALLCOUNTRIES_ERROR = "Either TP System or Country is null"; 
	public static String AUDIT_ALLTABCOL_ERROR = "Few tables are missing" ;
	public static String AUDIT_ALLTABLE_ERROR = "Few tables are missing" ;
	public static String AUDIT_METADATA_KEY = "MetaData_"; 
	public static String AUDIT_METAAPP_KEY = "MetaApp_"; 
	public static String HBASE_SERVER_IP = "HBASE_SERVER_IP";
	public static String HBASE_TABLE_NAME = "HBASE_TABLE_NAME";
	public static final String COULMN_FAMILY_DETAILED_INFO = "Detailed_Info";
	public static final String COULMN_FAMILY_BASIC_INFO = "Basic_Info";
	public static final String COULMN_JOBID = "JobId"; 
	public static final String COULMN_LOGS = "Logs"; 
	public static final String COULMN_DATE = "date"; 
	public static final String COULMN_ERRORS = "errors"; 
	public static final String COULMN_STATUS = "status"; 
	public static final String COULMN_GENERATESCHEMA = "generateschema"; 
	public static final String COULMN_READSOURCE = "readschema"; 
	public static final String COULMN_STORE = "writeoutput"; 

}
