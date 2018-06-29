/**
 * @CommonUtils: Utility class 
 * 1.file level utilities Read file,write file
 * 2.execute linux command through java
 * 3. Converting String to Map
 * @author Anurag Udasi
 */

package com.capgemini.mrapid.metaApp.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.MetaInfoConstants;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;

public class CommonUtils {
	final static Logger log = Logger.getLogger(CommonUtils.class);
	
	/**
	 * IO operation: Write File to local file system
	 * @param jsonString - Json as string as content of file
	 * @param filename: path with filename to be write
	 */
	public static void writeJSONFile(String jsonString, String filename) {
		try {
			File file = new File(filename);
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write(jsonString);
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Read File passed in argument
	 * @param filePath - path of file to read
	 * @return fileReader object
	 */
	public static FileReader readFile(String filePath) {
		
		FileReader reader = null;
		try {
			File inputFile = new File(filePath);
			reader = new FileReader(inputFile);
		} catch (FileNotFoundException e) {
				log.error("Property File not present at " + filePath);
				System.exit(1);
		}
		return reader;
	}

	/**
	 * Read schema file from given location
	 * @param filePath
	 * @param auditLogsPojoObj
	 * @return
	 */
	public static FileReader readInputSchemaFile(String filePath) {
		
		FileReader reader = null;
		String file_name = "";
		try {
			
			File inputFile = new File(filePath);
			if(inputFile.isDirectory()){
				File[] files = inputFile.listFiles();
				if(files != null){
					for(File f: files){
						file_name = f.getName();
			        }
			        inputFile =  new File(filePath + "" + file_name);   
				reader = new FileReader(inputFile);		
				
				}else{
					throw new FileNotFoundException();
				}
			}else{
				reader = new FileReader(inputFile);		
			} 
		} catch (FileNotFoundException e) {
				log.error("Source schema file not present at " + filePath);
		}
		return reader;
	}

	/**
	 * write error schema files into error file path
	 * @param srcSystem
	 * @param country
	 * @param errorTableList
	 * @param hadoopPropertiesMap
	 * @param countryMap
	 */
	public static void writeErrorFile(String srcSystem,String country,String errorTableList,Map<String,String> hadoopPropertiesMap,Map<String,String> countryMap){
		ArrayList<String> tableNames = new ArrayList<String>(Arrays.asList(errorTableList.split(",")));
		Iterator<String> iterator = tableNames.iterator();
		try{
			while (iterator.hasNext()) {
				String table = iterator.next();
				String inputPath = countryMap.get(Constants.LOCAL_INPUT_PATH) + "/" + table + "/" + hadoopPropertiesMap.get(Constants.SCHEMA_DIR);
				String outputPath = countryMap.get(Constants.ERROR_FILE_PATH) + "/" + srcSystem + "/" + country + "/" + table;
				File sourcePath = new File(inputPath);
				File targetPath = new File(outputPath);
				FileUtils.copyDirectory(sourcePath, targetPath);
			}
		}catch (Exception e){
			log.error(e.getMessage());
		}
	}

	
	/**
	 * Validate Table list for threshold values
	 * @param tableList
	 * @return
	 */
	public static boolean validateTableList(Map<String,TableListStructure> tableList){
		
		for(Map.Entry<String, TableListStructure> tempTableList : tableList.entrySet())
		{
			if(tempTableList.getValue().getThreshold() != null){
				String threshold = ParserUtils.cleanString(tempTableList.getValue().getThreshold());
				if(threshold != null & threshold.length() > 0){
					String temp = threshold.substring(threshold.length() - 1);
					if(!temp.equalsIgnoreCase("p") && !temp.equalsIgnoreCase("v")){
						return false;
					}
				}else{
					return false;
				}
			}else{
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Validate Excel sheet for header and footer
	 * @param metaInformationt
	 * @return
	 */
	public static boolean validateHeaderandFooter(Map<String,String> metaInformation,Map<String,String> sourcePropertyMap){
		if(sourcePropertyMap.get(Constants.IS_HEADER_FOOTER).equalsIgnoreCase("false"))
		{
			return true;
		}
		else if(metaInformation.containsKey(ExcelConstants.HEADER_FORMAT)){
			if(metaInformation.containsKey(ExcelConstants.FOOTER_FORMAT)){
				try{
					String header_value = metaInformation.get(ExcelConstants.HEADER_FORMAT);
					String footer_value = metaInformation.get(ExcelConstants.FOOTER_FORMAT);
					if(header_value.isEmpty() || footer_value.isEmpty()){
						return false;
					}else{
						boolean flag = validateLength(metaInformation);
						return flag;
					}
				}catch(Exception e){
					log.error(e.getMessage());
					return false;
				}	
			}else{
				return false;
			}
		}else{
			return false;
		}
	}
	
	/**
	 * Validates header, footer length
	 * @param metaInfo
	 * @return
	 */
	private static boolean validateLength(Map<String,String> metaInfo){
		if(isFixedWidth(metaInfo)){
			if(validateArray(metaInfo.get(ExcelConstants.HEADER_FORMAT))){
				if(validateArray(metaInfo.get(ExcelConstants.FOOTER_FORMAT))){
					return true;
				}else{
					return false;
				}
			}else{
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @param validateString
	 * @return
	 */
	private static boolean validateArray(String validateString){
		if(validateString.contains(":"))
		{
			String[] splitValidateString = validateString.split("\\,");
			for(int i = 0;i < splitValidateString.length;i++){
				if(splitValidateString[i].contains(":")){
					String[] tempString = splitValidateString[i].split("\\:");
					if(tempString.length == 2){
						if(!isInteger(tempString[1]))
							return false;
					}else{
						return false;
					}
				}else{
					return false;
				}
			}
			return true;
		}else{
			return false;
		}
	} 
	

	/**
	 * Updates header, footer values in json schema file
	 * @param oldSourceSchema
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param tableName
	 * @param excelPropertyMap
	 * @return
	 */
	public static boolean updateHeaderandFooterValueInJSON(JSONObject oldSourceSchema,Map<String,String> propertiesMap,String srcSystem,String country,String tableName,Map<String,String> excelPropertyMap){
		
		HDFSHandle hdfsHandler = new HDFSHandle();
		try {
			JSONObject metaInformation = oldSourceSchema.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
			JSONObject metaInfoSchemaUpdate = CommonUtils.updateHeaderAndFooter(metaInformation,excelPropertyMap);
			oldSourceSchema.put(JSONConstants.METAINFORMATION_SCHEMA, metaInfoSchemaUpdate);
			String jsonFilePath = propertiesMap.get(Constants.JSON_OUTPUT_DIR);
			String jsonFileName = srcSystem + "_" + country + "_" + tableName + Constants.JSON_FORMAT;
			boolean result = hdfsHandler.hdfsWrite(oldSourceSchema.toString(), propertiesMap,jsonFileName, jsonFilePath);
			if (result) {
				log.info("file created at " + jsonFilePath + "/" + jsonFileName);
			} else {
				return false;
			}

		} catch (JSONException e) {
			log.error(e.getMessage());
		} catch (IOException e){
			log.error(e.getMessage());
		}
		
		return true;
	}
	
	/**
	 * @param metaInfo
	 * @param excelPropertyMap
	 * @return
	 */
	public static JSONObject updateHeaderAndFooter(JSONObject metaInfo,Map<String,String> excelPropertyMap){
		
		JSONObject fianlMetaInfoObject = metaInfo;
		String header = excelPropertyMap.get(ExcelConstants.HEADER_FORMAT);
		String footer = excelPropertyMap.get(ExcelConstants.FOOTER_FORMAT);
		try{
		String headerColumn = getColumnName(header);
		String footerColumn = getColumnName(footer);
		if(isFixedWidth(excelPropertyMap)){
			String headerColumnPosition = getColumnPosition(header);
			String footerColumnPosition = getColumnPosition(footer);
			fianlMetaInfoObject.put(MetaInfoConstants.HEADER_COLUMN_POSITION, headerColumnPosition);
			fianlMetaInfoObject.put(MetaInfoConstants.FOOTER_COLUMN_POSITION, footerColumnPosition);
		}
		fianlMetaInfoObject.put(MetaInfoConstants.HEADER_FORMAT, headerColumn);
		fianlMetaInfoObject.put(MetaInfoConstants.FOOTER_FORMAT, footerColumn);
		fianlMetaInfoObject.put(MetaInfoConstants.HEADER_ORIGIN, header);
		fianlMetaInfoObject.put(MetaInfoConstants.FOOTER_ORIGIN, footer);
	
		if(fianlMetaInfoObject.has(MetaInfoConstants.HEADER_DATE_FORMAT)){
			if(!fianlMetaInfoObject.isNull(MetaInfoConstants.HEADER_DATE_FORMAT)){
				String temp = fianlMetaInfoObject.getString(MetaInfoConstants.HEADER_DATE_FORMAT);
				temp = temp.replace("mm", "MM");
				fianlMetaInfoObject.put(MetaInfoConstants.HEADER_DATE_FORMAT, temp);
			}
		}
		
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return fianlMetaInfoObject;
	}
	
	/**
	 * Check if format for data is fixedwidth
	 * @param execlPropertyMap
	 * @return
	 */
	private static boolean isFixedWidth(Map<String,String> execlPropertyMap){
		
		String header = execlPropertyMap.get(ExcelConstants.HEADER_FORMAT);
		if(!header.equalsIgnoreCase("null") && !header.isEmpty()){
			if(header.contains(":")){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * get column name from string
	 * @param name
	 * @return
	 */
	private static String getColumnName(String name){
		String columnName = "";
		if(name.contains(":")){
			String[] temp = name.split("\\,");
			if(temp.length > 0 ){
				for(int i = 0;i < temp.length;i++){
					String[] tempName = temp[i].split("\\:");
					if(tempName.length >0)
						columnName +=tempName[0] +",";
				}
				columnName = columnName.substring(0,columnName.length()-1);
			}
		}else{
			columnName = name;
		}
		return columnName;
	}
	
	/**
	 * get column position from structure
	 * @param position
	 * @return
	 */
	private static String getColumnPosition(String position){
		String columnPositionString = "";
		TreeMap<Integer,ColumnStructure> columnPosition = new TreeMap<Integer, ColumnStructure>();
		if(position.contains(":")){
			String[] temp = position.split("\\,");
			if(temp.length > 0 ){
				for(int i = 0;i < temp.length;i++){
					String[] tempName = temp[i].split("\\:");
					if(tempName.length >0){
						ColumnStructure col = new ColumnStructure();
						col.setColumnName(tempName[0]);
						col.setLength(Integer.parseInt(tempName[1]));
						columnPosition.put(i+1, col);
					}	
				}
			}
		}
		if(columnPosition.size() >= 1){
			columnPositionString = getColumnPositionFromMap(columnPosition);
		}
		return columnPositionString;
	}
	
	
	/**
	 * @param columnPosition
	 * @return
	 */
	public static String getColumnPositionFromMap(TreeMap<Integer,ColumnStructure> columnPosition){
		Integer startPoint = 0;
		Integer endPoint = 0;
		Integer totalLength = 0;
		String columnPositionString = "";
		
		for (Map.Entry<Integer, ColumnStructure> entry : columnPosition.entrySet()) {
			Integer columnLength = entry.getValue().getLength();
			endPoint = startPoint + (columnLength);
			totalLength = startPoint + columnLength;
			columnPositionString += startPoint +"-" + endPoint + ",";
			startPoint = totalLength;
		}
		columnPositionString = columnPositionString.substring(0,columnPositionString.length()-1);
		return columnPositionString;
	}
	
	/**
	 * @param metaInfo
	 * @param excelColumnStructure
	 * @return
	 */
	public static JSONObject updateDataColumnPosition(JSONObject metaInfo,ArrayList<ColumnStructure> excelColumnStructure){
		JSONObject updatedMetaInfo = metaInfo;
		TreeMap<Integer,ColumnStructure> columnPosition = new TreeMap<Integer, ColumnStructure>();
		try{
		for(int i = 0;i < excelColumnStructure.size(); i++){
			columnPosition.put(i+1, excelColumnStructure.get(i));
		}
		String columnPositionString = getColumnPositionFromMap(columnPosition);
		updatedMetaInfo.put(MetaInfoConstants.DATA_COLUMN_POSITION, columnPositionString);
		}catch(JSONException e){
			log.error(e.getMessage());
		}
		return updatedMetaInfo;
	}	
	
	/**
	 * @param str
	 * @return
	 */
	public static boolean isInteger(String str) {
	    try { 
	        Integer.parseInt(str); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    return true;
	}
}