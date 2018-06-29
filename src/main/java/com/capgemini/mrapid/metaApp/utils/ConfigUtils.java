package com.capgemini.mrapid.metaApp.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;





import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;
import com.capgemini.mrapid.metaApp.exceptions.EmptyFileExcetion;
import com.capgemini.mrapid.metaApp.exceptions.PropertyValueNotFoundException;
import com.capgemini.mrapid.metaApp.integration.impl.MysqlHandler;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.Generator;

/**
 * Class : File and System Utility class 
 * 1.read property files from resources folder
 * 2.read Application level properties files mention in resources/config file
 * 3.Constructing  Configuration values on basis of passed srcSystem county and table
 *
 * @author Anurag Udasi
 */

public class ConfigUtils {
	final static Logger log = Logger.getLogger(ConfigUtils.class);
	
	/** 
	 * Create table list for metaApp
	 * @param workbook
	 * @return
	 */
	public Map<String,TableListStructure> createTableList(Workbook workbook){
		Map<String, TableListStructure> tableMap = new HashMap<String, TableListStructure>();
		ArrayList<String> columnHeader = new ArrayList<String>();
		Sheet tableSheet = null;
		Integer count = 1;
		try {	
			tableSheet = workbook.getSheet(ExcelConstants.PRIMARY_KEY_SHEET_NAME);
			if(tableSheet != null){
				Iterator<Row> rowIterator = tableSheet.iterator();
				while (rowIterator.hasNext()) 
				{
					Row row = rowIterator.next();
					Iterator<Cell> cellIterator = row.cellIterator();
					TableListStructure tableListStructure = new TableListStructure();
					
					if(row.getRowNum() == 0){
						while (cellIterator.hasNext()) 
						{
							Cell cell = cellIterator.next();
							columnHeader.add(cell.getStringCellValue());		
						}
					}else{
						for (int columnCount=0; columnCount<columnHeader.size(); columnCount++) {
							Cell cell = row.getCell(columnCount, Row.RETURN_BLANK_AS_NULL);
							if (cell == null) {
								continue;
							} else {
								if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.PRIMARY_TABLE_NAME))
								{
									tableListStructure.setName(ParserUtils.cleanString(cell.getStringCellValue()));
			            		}
			            		else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.TYPE_OF_DATA))
			            		{
			            			tableListStructure.setClassification(ParserUtils.cleanString(cell.getStringCellValue()));
			            		}
			            		else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.THRESHOLD_LIMIT))
			            		{
			            			if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
				            			tableListStructure.setThreshold("");
			            			}else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
				            			if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
					            			tableListStructure.setThreshold("");
				            			else	
					            			tableListStructure.setThreshold(ParserUtils.cleanString(cell.getStringCellValue()));
				            		}else{
				            			Integer temp = (int) cell.getNumericCellValue();
				            			tableListStructure.setThreshold(temp.toString());      	
				            		}
			            		}
							}
						}
					if(!tableListStructure.getName().equalsIgnoreCase("")){
						count++;
						tableMap.put(count.toString(), tableListStructure);	
					}		
				}	
			}
		}else{
			throw new EmptyFileExcetion("Excel does not have Table list Information");
		}
			
			if(tableMap.size() < 1){
				throw new EmptyFileExcetion("Excel does not have Table list Information");
			}
				
		} catch (EmptyFileExcetion e){
			log.error(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		
		return tableMap;
		
	}
	
	/**
	 * Read table list provided for Junit test case
	 * @param listFile
	 * @return
	 */
	
	public Map<String, TableListStructure>  readTableList(String listFile) {
		Map<String, TableListStructure> tableMap = new HashMap<String, TableListStructure>();
		BufferedReader bufferReader = null;
		try {
			File inputFile = new File(listFile);
			FileReader inputFileReader = new FileReader(inputFile);
			bufferReader = new BufferedReader(inputFileReader);
			String sCurrentLine = "";
			String propertyName = "";

			while ((sCurrentLine = bufferReader.readLine()) != null) {
				String[] SplitString = sCurrentLine.split(",");
				if (SplitString.length >= 0) {
				 TableListStructure tableStructure = new TableListStructure();
				 tableStructure.setName(SplitString[1].toString());
				 tableStructure.setClassification(SplitString[2].toString());
				 propertyName = SplitString[0];
				 tableMap.put(propertyName, tableStructure);
				}
			}

			if(tableMap.isEmpty())
				throw new EmptyFileExcetion("Table List is empty...!");
			
		} catch (FileNotFoundException e) {
			log.error("Table List File file not found at " + listFile);
			System.exit(1);
		}catch(EmptyFileExcetion e){
			log.error(e.getMessage());
			System.exit(1);
		}catch (Exception e) {
			log.error(e.getMessage());
		} finally{
			if(bufferReader != null ){
				try {
					bufferReader.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}	
			}
		}
      
		return tableMap;
	}

	/**
	 * Create table list from excel sheet for metaApp
	 * @param workbook
	 * @return
	 */

	public Map<String,TableListStructure> createExcelTableList(Workbook workbook){
		Map<String, TableListStructure> tableMap = new HashMap<String, TableListStructure>();
		Sheet tableSheet = null;
		Integer count = 1;
		String tableName = null;
		ArrayList<String> columnHeader = new ArrayList<String>();
		
		try {	
			
			tableSheet = getTableSheet(workbook);
			
			if(tableSheet != null){
				Iterator<Row> rowIterator = tableSheet.iterator();
				while (rowIterator.hasNext()) 
				{
					
					TableListStructure tableListStructure = new TableListStructure();
					Row row = rowIterator.next();
					Iterator<Cell> cellIterator = row.cellIterator();
					if(row.getRowNum() == 0){
						while (cellIterator.hasNext()) 
						{
							Cell cell = cellIterator.next();
							columnHeader.add(ParserUtils.cleanString(cell.getStringCellValue()));		
						}
					}else{
						Cell name = row.getCell(0);
						tableName = ParserUtils.removeControlChar(name.getStringCellValue());
						if(!tableName.equalsIgnoreCase(""))
						{
							for (int columnCount=0; columnCount<columnHeader.size(); columnCount++) {
								Cell cell = row.getCell(columnCount, Row.RETURN_BLANK_AS_NULL);
								if (cell == null) {
									continue;
								} else {
									if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.TABLE_TYPE))
									{
										tableListStructure.setClassification(ParserUtils.cleanString(cell.getStringCellValue()));
											
									}else if(columnHeader.get(columnCount).equalsIgnoreCase(ExcelConstants.THRESHOLD_VALUE))
									{
							   			if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
					            			tableListStructure.setThreshold("");
				            			}else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
					            			if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
						            			tableListStructure.setThreshold("");
					            			else	
						            			tableListStructure.setThreshold(ParserUtils.cleanString(cell.getStringCellValue()));
					            		}else{
					            			Integer temp = (int) cell.getNumericCellValue();
					            			tableListStructure.setThreshold(temp.toString());      	
					            		}
											
									}
								}
							}	   
							tableListStructure.setName(tableName);
							tableMap.put(count.toString(), tableListStructure);
							count++;
						}
					}
				}
			}else{
				throw new EmptyFileExcetion("Excel does not have Table meta Information");
			}
				
		} catch (EmptyFileExcetion e){
			log.error(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		
		return tableMap;
		
	}
	
	/**
	 * Read excel file and return workbook object
	 * @param filePath
	 * @return
	 */
	public Workbook readExcelFile(String filePath){
		Workbook workbook = null;
		try {
			if (FilenameUtils.getExtension(filePath).equalsIgnoreCase("xls")) {
				workbook = new HSSFWorkbook(new FileInputStream(filePath));
			} else if (FilenameUtils.getExtension(filePath).equalsIgnoreCase("xlsx")) {
				workbook = new XSSFWorkbook(new FileInputStream(filePath));
			} else {
				throw new IllegalArgumentException(filePath +  "File does not have a standard excel extension.");
			}
						
		} catch (FileNotFoundException e) {
			log.error("Table List File not found at " + filePath);
			System.exit(1);
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		return workbook;	
}
	
	/**
	 * Create property map from excel sheet for given table
	 * @param workbook
	 * @param tableName
	 * @return
	 */
	public Map<String,String> createExcelPropertyMap(Workbook workbook,String tableName){
		Map<String, String> tableMap = new HashMap<String, String>();
		ArrayList<String> rowHeader = new ArrayList<String>();
		Sheet excelPropertyTableSheet = null;
		try {	
			
			excelPropertyTableSheet = getTableSheet(workbook);
			
			if(excelPropertyTableSheet != null){
				Iterator<Row> rowIterator = excelPropertyTableSheet.iterator();
				
				while (rowIterator.hasNext()) 
				{
					Row row = rowIterator.next();
					Iterator<Cell> cellIterator = row.cellIterator();
					Cell cellTableName = row.getCell(0);
					
					
					if(row.getRowNum() == 0){
						while (cellIterator.hasNext()) 
						{
							Cell cell = cellIterator.next();
							rowHeader.add(ParserUtils.cleanString(cell.getStringCellValue()));		
						}
					}else{
						String tempTableName = ParserUtils.removeControlChar(cellTableName.getStringCellValue());
						if(tempTableName.equalsIgnoreCase(tableName)){
							for (int columnCount=0; columnCount<rowHeader.size(); columnCount++) {
								Cell cell = row.getCell(columnCount, Row.RETURN_BLANK_AS_NULL);
								if (cell == null) {
									continue;
								} else {
									if( cell.getCellType() == Cell.CELL_TYPE_BLANK ){
										tableMap.put(rowHeader.get(columnCount),""); 
									}else if(cell.getCellType() == Cell.CELL_TYPE_STRING){
										if(ParserUtils.removeControlChar(cell.getStringCellValue()).equalsIgnoreCase(""))
											tableMap.put(rowHeader.get(columnCount),""); 
										else	
											tableMap.put(rowHeader.get(columnCount),ParserUtils.cleanString(cell.getStringCellValue())); 
									}else{
										Integer temp = (int) cell.getNumericCellValue();
										tableMap.put(rowHeader.get(columnCount),ParserUtils.cleanString(temp.toString())); 
									}
								}
							}
							break;
						}
					}	
				}
			}else{
				throw new EmptyFileExcetion("Excel does not have Table meta Information");
			}
				
		} catch (EmptyFileExcetion e){
			log.error(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		
		return tableMap;		
	}
	
	/**
	 * Getting Application level required Configurations form propertyFile passed as parameter
	 * @param srcSystem :srcSystem for which operation is doing
	 * @param country :country for which operation is doing
	 * @param propertyFile : Application propertyFile path
	 * @return returns the map contains all required constructed mapProperty() function
	 */
	public Map<String, String> readPropertyFile(String srcSystem, String country,String propertyFile) {
		Map<String, String> tableMap = new HashMap<String, String>();
		BufferedReader bufferReader = null;
		try {			
			if(propertyFile == null)
				throw new PropertyValueNotFoundException("File Path is not set in config file ");
			File inputFile = new File(propertyFile);
			bufferReader = new BufferedReader(new FileReader(inputFile));
			String sCurrentLine = "";
			String propertyValue = "";
			String propertyName = "";

			while ((sCurrentLine = bufferReader.readLine()) != null) {
				if (sCurrentLine.contains("#") || sCurrentLine.trim().isEmpty())
					continue;

				String[] SplitString = sCurrentLine.split("=",2);
				if (SplitString.length >= 0) {
					propertyName = SplitString[0];
					if(SplitString.length <= 1)
						throw new PropertyValueNotFoundException("Property value is not set for " + propertyName);
					propertyValue = SplitString[1];
					tableMap.put(propertyName, propertyValue);
				}
			}
		
			if(tableMap.isEmpty())
				throw new EmptyFileExcetion("Property file "+ propertyFile + "is empty...!");
		
		} catch (FileNotFoundException e){
			log.error("Property file " + propertyFile + "not present...!");
			
			System.exit(1);
		}catch(EmptyFileExcetion e){
			log.error(e.getMessage());
			System.exit(1);
		}catch (PropertyValueNotFoundException e){
			log.error(e.getMessage());
			
			System.exit(1);
		}catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(bufferReader != null ){
				try {
					bufferReader.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}	
			}
		}
		return tableMap;
	}
		
	
	/**
	 * Constructing Application level required Configurations on basis of passed srcSystem,table and country
	 * @param countryMap
	 * @param sourceMap
	 * @param hadoopMap
	 * @param table
	 * @param srcSystem
	 * @param country
	 * @return returns the map contains all required hdfs and local values constructed from passed srcSystem,table and country values
	 */
	public Map<String, String> mapProperty(Map<String, String> countryMap,Map<String, String> sourceMap,Map<String, String> hadoopMap, String table, String srcSystem,String country,String partition_col) {
		Map<String, String> finalPropertyMap = new HashMap<String, String>();
		try {
			  finalPropertyMap.put(Constants.HDFS_URL, hadoopMap.get(Constants.HDFS_URL));
			  
			  finalPropertyMap.put(Constants.ENV, hadoopMap.get(Constants.ENV));
              
			  finalPropertyMap.put(Constants.LOCAL_BASE_PATH,hadoopMap.get(Constants.LOCAL_BASE_PATH));
              
              finalPropertyMap.put(Constants.LOCAL_INPUT_PATH,countryMap.get(Constants.LOCAL_INPUT_PATH)  + "/" + table);
              
              finalPropertyMap.put(Constants.HDFS_BASE_PATH,sourceMap.get(Constants.HDFS_BASE_PATH) + "/" + country + "/" + table);
                          
              finalPropertyMap.put(Constants.HIVE_STAGING_DB, sourceMap.get(Constants.HIVE_STAGING_DB));
              
              finalPropertyMap.put(Constants.HIVE_PROCESSING_DB, sourceMap.get(Constants.HIVE_PROCESSING_DB));
              
              finalPropertyMap.put(Constants.HIVE_BASE_PATH_STAGING,sourceMap.get(Constants.HIVE_BASE_PATH) + "/" + sourceMap.get(Constants.HIVE_STAGING_DB) + "/" + country + "/" + srcSystem+"_"+country+"_"+table);
              
              finalPropertyMap.put(Constants.HIVE_BASE_PATH_PROCESSING,sourceMap.get(Constants.HIVE_BASE_PATH) + "/" + sourceMap.get(Constants.HIVE_PROCESSING_DB) + "/" + country + "/" + srcSystem + "_" + country + "_" + table);
              
              finalPropertyMap.put(Constants.HIVE_BASE_PATH_TEMP,sourceMap.get(Constants.HIVE_BASE_PATH) + "/" + sourceMap.get(Constants.HIVE_TEMP_DB) + "/" + country + "/" + srcSystem + "_" + country + "_" + table);
              
              finalPropertyMap.put(Constants.TARGET_DB_TYPE, sourceMap.get(Constants.TARGET_DB_TYPE));
              
              finalPropertyMap.put(Constants.TARGET_DB, sourceMap.get(Constants.TARGET_DB));
              
              finalPropertyMap.put(Constants.TARGET_DELIMETED, sourceMap.get(Constants.TARGET_DELIMETED));
              
              finalPropertyMap.put(Constants.TARGET_DB_FORMAT, sourceMap.get(Constants.TARGET_DB_FORMAT));
              
              finalPropertyMap.put(Constants.TARGET_PROCESSED_FORMAT, sourceMap.get(Constants.TARGET_PROCESSED_FORMAT));
              
              finalPropertyMap.put(Constants.TARGET_DATE_FORMAT, countryMap.get(Constants.TARGET_DATE_FORMAT));
              
              finalPropertyMap.put(Constants.TARGET_TIME_FORMAT, countryMap.get(Constants.TARGET_TIME_FORMAT));
               
              finalPropertyMap.put(Constants.SCHEMA_DIR,finalPropertyMap.get(Constants.LOCAL_INPUT_PATH) + "/" + hadoopMap.get(Constants.SCHEMA_DIR) + "/");
              
              finalPropertyMap.put(Constants.JSON_OUTPUT_DIR,finalPropertyMap.get(Constants.HDFS_BASE_PATH) + "/" + hadoopMap.get(Constants.JSON_OUTPUT_DIR));
              
              finalPropertyMap.put(Constants.AVSC_OUTPUT_DIR, finalPropertyMap.get(Constants.HDFS_BASE_PATH) + "/" + hadoopMap.get(Constants.AVSC_OUTPUT_DIR));
              
              finalPropertyMap.put(Constants.DDL_OUTPUT_DIR, finalPropertyMap.get(Constants.HDFS_BASE_PATH) +  "/" + hadoopMap.get(Constants.DDL_OUTPUT_DIR));
             
              finalPropertyMap.put(Constants.ENCODINGCHARSET,sourceMap.get(Constants.ENCODINGCHARSET));
              
              finalPropertyMap.put(Constants.ORC_COMPRESSION,sourceMap.get(Constants.ORC_COMPRESSION));
              
              finalPropertyMap.put(Constants.ERROR_FILE_PATH, countryMap.get(Constants.ERROR_FILE_PATH));
              
              finalPropertyMap.put(Constants.SUPPLEMENTARY_PK, countryMap.get(Constants.SUPPLEMENTARY_PK));
              
              finalPropertyMap.put(Constants.FDVALUE, sourceMap.get(Constants.FDVALUE));
              
              finalPropertyMap.put(Constants.FDFLAG, sourceMap.get(Constants.FDFLAG));
              
              finalPropertyMap.put(Constants.SCHEMA_EVAL_LOG_FILE_PATH, hadoopMap.get(Constants.SCHEMA_EVAL_LOG_FILE_PATH));
                            
              finalPropertyMap.put(Constants.HDFS_ARCHIVE_PATH, sourceMap.get(Constants.HDFS_ARCHIVE_PATH));
              
              finalPropertyMap.put(Constants.CDC_COULMN, sourceMap.get(Constants.CDC_COULMN));
              
              finalPropertyMap.put(Constants.IS_HEADER_FOOTER, sourceMap.get(Constants.IS_HEADER_FOOTER));
              finalPropertyMap.put(Constants.HIVE_TEMP_DB, sourceMap.get(Constants.HIVE_TEMP_DB));
             
              if(sourceMap.get(Constants.SOURCE_INPUT_TYPE).equalsIgnoreCase(Constants.RDBMS_SOURCE)){
            	  if(partition_col.equalsIgnoreCase(""))
            		  finalPropertyMap.put(Constants.TABLE_PARTITION, sourceMap.get(Constants.TABLE_PARTITION));
            	  else	  
            		  finalPropertyMap.put(Constants.TABLE_PARTITION, partition_col);
            	  finalPropertyMap.put(Constants.MYSQL_CONNECTION_URL, sourceMap.get(Constants.MYSQL_CONNECTION_URL));
            	  finalPropertyMap.put(Constants.MYSQL_USER, sourceMap.get(Constants.MYSQL_USER));
            	  finalPropertyMap.put(Constants.MYSQL_PASSWORD, sourceMap.get(Constants.MYSQL_PASSWORD));
            	  finalPropertyMap.put(Constants.MYSQL_DRIVER_CLASS, sourceMap.get(Constants.MYSQL_DRIVER_CLASS));
            	  finalPropertyMap.put(Constants.SOURCE_METAAPP_DEV_OPS, sourceMap.get(Constants.SOURCE_METAAPP_DEV_OPS));
            	  
			  } else{
            	  finalPropertyMap.put(Constants.TABLE_PARTITION, sourceMap.get(Constants.TABLE_PARTITION));
            	  finalPropertyMap.put(Constants.UNIQUE_ID_COLUMN, sourceMap.get(Constants.UNIQUE_ID_COLUMN));
                  finalPropertyMap.put(Constants.JOURNAL_TIME_COLS, sourceMap.get(Constants.JOURNAL_TIME_COLS));
                  finalPropertyMap.put(Constants.BUSS_JOURNAL_DATE_TIME_COLS, sourceMap.get(Constants.BUSS_JOURNAL_DATE_TIME_COLS));
                  finalPropertyMap.put(Constants.BUSS_DATE_COLS, sourceMap.get(Constants.BUSS_DATE_COLS));
              }
              
              
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		return finalPropertyMap;
	}
	
	/**
	 * Reading resources/Config properties files 
	 * @param configPath
	 * @return returns the Properties class object contains path of Application level Configurations files 
	 *  */
	public Properties getConfigValues(String configPath){
		final Properties props = new Properties();
		FileInputStream file = null;
		try {
			file = new FileInputStream(configPath); 	
			props.load(file);			
		} catch (Exception e) {
			log.error("Exception: " + e.getMessage());
			System.exit(1);
		} finally{
			if(file != null){
				try {
					file.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
		return props;
	}
	public Properties getConfigValues(){
		InputStream inputStream = null;
		Properties prop = new Properties();

		try {
			String propFileName = "config.properties";

			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			inputStream.close();
			
		} catch (Exception e) {
			log.error("Exception: " + e.getMessage());
			System.exit(1);
		} finally{
			if(inputStream != null){
				try {
					inputStream.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
		return prop;
	}
	
	/**
	 * Get tableSheet from list of sheets
	 * @param workbook
	 * @return
	 */
	private Sheet getTableSheet(Workbook workbook){
		String sheetName = new String();
		Sheet tableSheet = null;
		for ( int i = 0; i < workbook.getNumberOfSheets(); i++)
		{
			String tableSheetName = workbook.getSheetName(i).toLowerCase(); 
			if(tableSheetName.contains("table"))
			{
				sheetName = workbook.getSheetName(i);
				tableSheet = workbook.getSheet(sheetName);
				break;
			}
		}
		return tableSheet;
	}
	
	public Map<String,TableListStructure> createTableListFromDB(Map<String,String> sourceProperty,String srcSystem){
	
		String decryptPassword = Decrypt.decrypt(sourceProperty.get(Constants.MYSQL_PASSWORD), Generator.secretKey);
		MysqlHandler mysqlHandlerDevOps = new MysqlHandler(sourceProperty,sourceProperty.get(Constants.SOURCE_METAAPP_DEV_OPS),decryptPassword);
		Connection devOpsConnection = mysqlHandlerDevOps.getConnection();
		
		Statement stmt = null;
		Map<String,TableListStructure> srcTableListInfo = new HashMap<String, TableListStructure>();
		try{
			
			stmt = devOpsConnection.createStatement();
			ResultSet res = stmt.executeQuery("SELECT SRC_TABLE_NAME,TRGT_HIVE_PARTITION_COLS,HIVE_PART_COL_FLG FROM TABLE_DETAILS WHERE SRC_NAME='"+srcSystem+"'");
					
			while(res.next()){
				TableListStructure jobDetails = new TableListStructure();
				jobDetails.setName(res.getString("SRC_TABLE_NAME"));
				jobDetails.setClassification(res.getString("TRGT_HIVE_PARTITION_COLS"));
				jobDetails.setPartition_col(res.getString("TRGT_HIVE_PARTITION_COLS"));
				jobDetails.setPartition_col_val(res.getInt("HIVE_PART_COL_FLG"));
				
				srcTableListInfo.put(res.getString("src_table_name"), jobDetails);
				
			}
			
		}catch(Exception e){
			System.out.println(e.getMessage());
		}finally{
			mysqlHandlerDevOps.closeConnection(devOpsConnection);			
		}
		
		return srcTableListInfo;
	}
}


