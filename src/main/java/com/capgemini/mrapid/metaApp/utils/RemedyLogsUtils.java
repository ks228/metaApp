/**
 * Class : Remedy Utility class
 * This class will capture errors across components and log them in file as remedy 
 * 1.Read directory from properties file
 * 2.Create file in append mode with naming convention <srcSystem_country_datetime>.txt 
 * 3.Log errors occurred while processing input files in <srcSystem_country_datetime>.txt in below format
 *     - Component
 *     - Date/time
 *	   - Error/Exception
 *     - Product Code
 * @author Anuradha Dede
 */

package com.capgemini.mrapid.metaApp.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.constants.RemedyConstants;

public class RemedyLogsUtils {
	final static Logger log = Logger.getLogger(RemedyLogsUtils.class);

	static FileWriter fw = null;
	static public Map<String, String> remedyLogsPropertiesMap = new HashMap<String, String>();
	
	/**
	 * Reads input file and generates map of errorCodes and its values
	 * @param srcSystem
	 * @param country
	 * @param errorCodePath
	 * @return
	 */
	public static void createMapProductErrorCode(String srcSystem, String country, String errorCodePath)
	{
		ConfigUtils config = new ConfigUtils();
		remedyLogsPropertiesMap = config.readPropertyFile(srcSystem,country,errorCodePath);
	}


	/**
	 * Create file for logging error in append mode
	 * @param dirPath
	 * @param srcSystem
	 * @param country
	 * @param dateFormat
	 * @param fileExt
	 * @param productErrorCode
	 * @return
	 * @throws IOException 
	 */
	public static void createRemedyLogsFile(String dirPath, String srcSystem, String country,String dateFormat,String fileExt,String productErrorCode ) throws IOException
	{
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		SimpleDateFormat sdf1 = new SimpleDateFormat("HH24:MM:SS");
		Calendar c1 = Calendar.getInstance();
		String date = sdf.format(c1.getTime());
		Calendar c2 = Calendar.getInstance();
		String timestamp = sdf1.format(c2.getTime());
		String fileName =  srcSystem + "_" + country + "_" +RemedyConstants.REMEDY_METAAPP+productErrorCode+"_"+date +"_"+timestamp+"."+fileExt;
		String filePath = dirPath+"/"+RemedyConstants.REMEDY_PARTITION+date+"/"+fileName;
		log.info("#####inside createRemedyLogsFile######"+filePath);
		File file = new File(filePath);
		
		// if file not exists, then create it
		try{
				file.getParentFile().mkdirs();
//				FileWriter writer = new FileWriter(file);
				file.createNewFile();
				fw = new FileWriter(new File(filePath));
				//using PosixFilePermission to set file permissions 777
		        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
		        //add owners permission
		        perms.add(PosixFilePermission.OWNER_READ);
		        perms.add(PosixFilePermission.OWNER_WRITE);
		        perms.add(PosixFilePermission.OWNER_EXECUTE);
		        //add group permissions
		        perms.add(PosixFilePermission.GROUP_READ);
		        perms.add(PosixFilePermission.GROUP_WRITE);
		        perms.add(PosixFilePermission.GROUP_EXECUTE);
		        //add others permissions
		        perms.add(PosixFilePermission.OTHERS_READ);
		   
		        perms.add(PosixFilePermission.OTHERS_EXECUTE);
		      
				Files.setPosixFilePermissions( Paths.get(filePath), perms);
			} catch (IOException e) {
				e.printStackTrace();
			}

	}



	/**
	 * Write error logs in file 
	 * @param component
	 * @param srcSystem
	 * @param country
	 * @param productErrorCode
	 * @param exceptionName
	 * @param exceptionDetail
	 * @return
	 */
	public static void writeToRemedyLogs(String component, String srcSystem, String country, String productErrorCode, String exceptionName, String exceptionDetail ,Map<String,String> deploymentProperties) {

		try {
			
			log.info("#####inside writeToRemedyLogs######");
			createRemedyLogsFile(deploymentProperties
					.get(RemedyConstants.REMEDY_ERROR_LOGS_PATH),srcSystem,country,deploymentProperties
							.get(RemedyConstants.REMEDY_FILE_DATEFORMAT),deploymentProperties
									.get(RemedyConstants.REMEDY_FILE_EXTENSION),productErrorCode);
			String delim=StringEscapeUtils.unescapeJava("\u00A7"); 
			if(exceptionDetail==null)
			{
				exceptionDetail="Warning";
			}
			String remdymessage=productErrorCode + delim + exceptionName + delim + exceptionDetail;
			BufferedWriter br = new BufferedWriter(fw);
			br.write(remdymessage);
			br.flush();
			br.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Close BufferedReader and FileWriter handle
	 * @return
	 */
	public static void closeRemedyLogFileHandle(){
		try {
			if (fw != null)
				fw.close();
		} 
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
