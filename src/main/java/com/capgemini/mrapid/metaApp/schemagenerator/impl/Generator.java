/**
 * Generator
 * It implements IGenerator Interface which implements generate method 
 * It Reads table list and for that table it reads XML,Excel etc schema and creates JSON, HQL and AVSC 
 * Files for latest version from MetaData
 * @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONObject;


//import com.capgemini.mrapid.auditlogs.integration.impl.AuditLoghandler;
//import com.capgemini.mrapid.auditlogs.pojo.AuditLogPojo;
//import com.capgemini.mrapid.metaApp.audit.AuditClient;
//import com.capgemini.mrapid.metaApp.constants.AuditConstants;
import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.RemedyConstants;
import com.capgemini.mrapid.metaApp.exceptions.HDFSFileOperationException;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.IGenerator;
import com.capgemini.mrapid.metaApp.utils.CommonUtils;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;
import com.capgemini.mrapid.metaApp.utils.JsonUtils;
import com.capgemini.mrapid.metaApp.utils.MetaInfoGenerator;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.capgemini.mrapid.metaApp.utils.ReconTableCreation;
import com.capgemini.mrapid.metaApp.utils.SchemaEvolution;
import com.capgemini.mrapid.metaApp.utils.RemedyLogsUtils;

public class Generator implements IGenerator {
	final static Logger log = Logger.getLogger(Generator.class);

	Map<String, String> propertiesMap = new HashMap<String, String>();
	Map<String, String> hadoopPropertiesMap = new HashMap<String, String>();
	Map<String, String> dataTypeMappingPropertiesMap = new HashMap<String, String>();
	Map<String, String> deploymentProperties = new HashMap<String, String>();
	Map<String, String> sourcePropertiesMap = new HashMap<String, String>();
	Map<String, String> countryPropertiesMap = new HashMap<String,String>();
	String errorTableList = new String();
	StringBuilder newVersionTableList = new StringBuilder();
	Workbook workbook = null;
	String inputDataType = "";
//	AuditLoghandler auditHandler = new AuditLoghandler();
	String serverIP = null;
	String auditTableName = null;
	boolean isSchemaEvolution = true;
	public Map<String, String> basicInfo = new HashMap<String, String>();
	public final static String secretKey = "mRapid";
	

	/**
	 * Reads input file and generates schema and stores it. Calls
	 * AuditLogHandler to make an entry for each table in Hbase
	 * @param srcSystem
	 * @param country
	 * @param configPath
	 * @return
	 */
	public boolean generate(String srcSystem, String country, String configPath) {

		try {
//			AuditLogPojo auditLogsPojo = new AuditLogPojo();
			ConfigUtils config = new ConfigUtils();
			Properties property = config.getConfigValues(configPath);
			Map<String, TableListStructure> tableListMap = new HashMap<String, TableListStructure>();
			System.out.println("In generator");
			//Initialize all property maps
			initialization(srcSystem,country,config,property);
			System.out.println("In generator");
			//Check for required property values
			propertyCheck(srcSystem,country,config,property);
			
			System.out.println("In generator");
			
            // setting flag for schema evolution
			if (sourcePropertiesMap.get(Constants.IS_SCHEMA_EVOLUTION).equalsIgnoreCase("enable")) {
				if (countryPropertiesMap.get(Constants.IS_SCHEMA_EVOLUTION).equalsIgnoreCase("enable"))
					isSchemaEvolution = true;
				else
					isSchemaEvolution = false;
			} else {
				isSchemaEvolution = false;
			}

			//Create tableList work book TODO - create new private method for this
			tableListMap = createTableListGenerator(srcSystem, country, config, property);
			//Validate tableList for Threshold and classification
			//*********************************************************************************
			//This block of code will validate table list for P and V in threshold values
			if(!inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE)){
				boolean tableListValidflag = CommonUtils.validateTableList(tableListMap);
				if(!tableListValidflag){
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYSEVEN,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYSEVEN),"Table list is invalid, Contains wrong threashold value",deploymentProperties);
					RemedyLogsUtils.closeRemedyLogFileHandle();
					System.exit(1);
				}		
			}
			//*********************************************************************************
			
			//Check weather schema creation is already created for country
			int allCntryVersion = SchemaEvolution.getSchemaCntryVersion(hadoopPropertiesMap, srcSystem, country,deploymentProperties);
			//int allCntryVersion = 0;
			log.info(srcSystem + ":" + country + "MetaApp allCntryVersion:"+allCntryVersion);
			
			if((allCntryVersion>=1) && (!isSchemaEvolution))
			{
				SchemaEvolution.writeSchemaTableList(newVersionTableList,
						hadoopPropertiesMap, srcSystem, country,deploymentProperties);
				log.info(srcSystem + ":" + country + ":SchemaEvolution  is disbale  at country or srcSystem level and also intial deployment is completed earlier...So exiting MetaApp.Please enable the SchemaEvolution flag in property file and try Again ");
				RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYTWO,
						RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYTWO),null,deploymentProperties);
				RemedyLogsUtils.closeRemedyLogFileHandle();
				System.exit(1);
			}
			
			//Create Map of table and schema version
			Map<String,Integer> schemaVersionMap = new HashMap<String,Integer>(); 
			log.info("Look for latest version of table from MetaData");
			
			//schemaVersionMap = SchemaEvolution.getSchemaVersionMap(hadoopPropertiesMap, srcSystem, country,deploymentProperties);
			
			for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
				System.out.println("TABLE IS : " + table.toString());
				JSONObject metaInformation = new JSONObject();
				JSONObject schemaInformationObject = null;
				FileReader schemaFile = null;
				Map<String, String> excelPropertyMap = new HashMap<String, String>();
				Timestamp timestamp = new Timestamp(System.currentTimeMillis());
				Date date = new Date(timestamp.getTime());
				Integer schemaVersion = null;
				String threshold = "";
				String tableName = ParserUtils.removeControlChar(table.getValue().getName());
				String classification = ParserUtils.removeControlChar(table.getValue().getClassification());
				String partition_col = new String();
				Integer partition_col_val = 0;
				//Assign threshold value of table to local variable
				//************************************************************************
				if(!inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE)){					
					threshold = ParserUtils.removeControlChar(table.getValue().getThreshold());
				}else{
					if(table.getValue().getPartition_col().equalsIgnoreCase("") || table.getValue().getPartition_col().isEmpty() || table.getValue().getPartition_col() == null)
						partition_col = "";
					else	
						partition_col = ParserUtils.removeControlChar(table.getValue().getPartition_col());
					
					partition_col_val = table.getValue().getPartition_col_val();
					threshold = "99P";
				}
				//************************************************************************
				
				if(schemaVersionMap.containsKey(tableName)){
					schemaVersion = schemaVersionMap.get(tableName);
				}else{
					schemaVersion = 1;
				}
				log.info("Partition value for table :- " + tableName + " is " + partition_col_val + " and partition col name is " + partition_col);
				
				//Create audit pojo and key
//				String key = AuditConstants.AUDIT_METAAPP_KEY + srcSystem + "_" + country + "_" + "TL" + "_" + tableName + timestamp;
//				auditLogsPojo.setDate(date);
//				auditLogsPojo.setKey(key);
//				auditLogsPojo.setJobId();
//				auditLogsPojo.setLogs(deploymentProperties.get(Constants.LOG_FILE_PATH));
//				basicInfo.put(AuditConstants.COULMN_DATE, date.toString());
//				basicInfo.put(AuditConstants.COULMN_LOGS,deploymentProperties.get(Constants.LOG_FILE_PATH));
//				basicInfo.put(AuditConstants.COULMN_JOBID,auditLogsPojo.getJobId());

				// Map all property file to one Hash Map
				log.info(srcSystem + ":" + country + ":" + tableName+ "##################Table::" + tableName+ "###################");
				propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap, hadoopPropertiesMap, tableName,srcSystem, country,partition_col);
				// Generate meta information JSON
				log.info(srcSystem + ":" + country + ":" + tableName + ":reading metaInfoFile...... " + property.getProperty(srcSystem.toUpperCase() + Constants.PROPERTIES_PATH) + "");
				if (inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE)) {
					excelPropertyMap = config.createExcelPropertyMap(workbook,tableName);
					metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(excelPropertyMap, sourcePropertiesMap,table.getValue(), country);
					//*******************************************************************************
					Boolean validate_MetaInfo = CommonUtils.validateHeaderandFooter(excelPropertyMap,sourcePropertiesMap);
					//*******************************************************************************
					
					if(validate_MetaInfo){
						log.info(srcSystem + ":" + country + ":" + tableName + ":reading  Read schema file from ..... " + property.getProperty(srcSystem.toUpperCase() + "_"
								+ country.toUpperCase()
								+ Constants.INPUT_FILE_PATH) + "");
						schemaFile = CommonUtils.readInputSchemaFile(property.getProperty(srcSystem.toUpperCase() + "_" + country.toUpperCase() + Constants.INPUT_FILE_PATH));
					}else{
						log.warn("Wrong header/footer value encounterd in " + tableName);
						errorTableList +=tableName + ",";
						RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYEIGHT,
								RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYEIGHT),tableName,deploymentProperties);
						continue;
					}
				} else if(inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE)){
					metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap,
							propertiesMap, table.getValue(), country);
				} else {
					metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap,
							propertiesMap, table.getValue(), country);
					log.info(srcSystem + ":" + country + ":" + tableName + ":reading  Read schema file from ..... "
							+ propertiesMap.get(Constants.SCHEMA_DIR) + "");
					schemaFile = CommonUtils.readInputSchemaFile(propertiesMap.get(Constants.SCHEMA_DIR));
				}

				// Read Schema File create JSON object
				log.info(srcSystem + ":" + country + ":" + tableName + ":Read Schema File.... ");
				schemaInformationObject = readSchemaFileGenerator(srcSystem,country,tableName,classification,metaInformation,schemaFile,excelPropertyMap);
				if (schemaInformationObject != null) {
					Boolean isValidJSON = JsonUtils.isValidJson(schemaInformationObject);
					if (isValidJSON) {
						boolean isRetrofit = false;
						List<String> addedColumnList = new ArrayList<String>();
							
						//Schema creation for recon tables
						if(classification.equalsIgnoreCase(Constants.TABLE_TYPE_RECON) && schemaVersion == 1){
							createReconTableGenerator(tableName,country,srcSystem,schemaInformationObject);
							newVersionTableList.append(SchemaEvolution.createSchemaTableList(tableName,classification,schemaVersion,false));
							continue;
						}
							
						//Schema creation for general tables
						if (schemaVersion == 1) {
							log.info("Run schema creation for first time");
							isRetrofit = generateSchema(srcSystem, country, table,schemaInformationObject,schemaVersion, false,null,deploymentProperties,addedColumnList,partition_col_val);
							newVersionTableList.append(SchemaEvolution.createSchemaTableList(tableName,classification,schemaVersion,isRetrofit));
						} else if (schemaVersion > 1 && !classification.equalsIgnoreCase(Constants.TABLE_TYPE_RECON)) {
							log.info("Run schema evolution with latest version :- " + schemaVersion);
							JSONObject oldSourceSchema = SchemaEvolution.getOldSourceSchema(srcSystem, country,tableName, propertiesMap,schemaVersion);
							if(oldSourceSchema == null){  
								errorTableList +=tableName + ",";
								RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYSIX,
										RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYSIX),tableName,deploymentProperties);
								continue;
							}
							//Compare Old and New Threshold value for change
							//**********************************************************************************
							log.info("Compare JSON for Threshold value changes");
							Boolean isOnlyThresholdChange = SchemaEvolution.compareThresholdValues(oldSourceSchema,threshold);
							//**********************************************************************************
				
							//Compare Old and New Header and footer value for change in Excel
							//**********************************************************************************
							log.info("Compare JSON for Header and footer value changes");
							Boolean isOnlyHeaderFooterChange = true;
							if(inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE)){
								///work on this
								isOnlyHeaderFooterChange = SchemaEvolution.compareHeaderandFooterValues(oldSourceSchema,excelPropertyMap);
							}
							//**********************************************************************************
							
							log.info("Compare JSON for schema changes");
							Boolean schemaCheckFlag = SchemaEvolution.compareSourceSchema(oldSourceSchema,schemaInformationObject);
							if (!schemaCheckFlag) {
								log.info("update new json for default value");
								schemaInformationObject = SchemaEvolution.getUpdatedJson(oldSourceSchema, schemaInformationObject,dataTypeMappingPropertiesMap.get(Constants.AVRO_INCOMPATIBLE_TYPES));
								if(schemaInformationObject == null){
									errorTableList +=tableName + ",";
									RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYTHREE,
											RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYTHREE),tableName,deploymentProperties);
								}else{
									log.info("move previous version file to ");
									SchemaEvolution.moveOldJSONFile(schemaVersion, propertiesMap,srcSystem, country, tableName);
									log.info("Change existing schema with new");
									isRetrofit = generateSchema(srcSystem, country, table,schemaInformationObject,schemaVersion,true, oldSourceSchema,deploymentProperties,addedColumnList,partition_col_val);
									log.info("Move old schema to archive");
									SchemaEvolution.moveOldSchema(schemaVersion, propertiesMap,srcSystem, country, tableName);
									newVersionTableList.append(SchemaEvolution.createSchemaTableList(tableName,classification,schemaVersion,isRetrofit));
								}
							}
							//Update old JSON if its just threshold change
							//************************************************************************************
							else if(!isOnlyThresholdChange){
								log.info("Only Threshold value has changed");
								log.info("Updating existing JSON with new Threshold value");
								boolean flag = SchemaEvolution.updateThresholdValueInJSON(oldSourceSchema, propertiesMap,srcSystem, country, tableName,threshold);
								if(!flag){
									log.error("Fail to update Threshold value");
									errorTableList += tableName + ",";
								}			
							}
							//************************************************************************************
							
							//Update old JSON if its just header or footer change in EXCEL sheet
							//************************************************************************************
							else if(!isOnlyHeaderFooterChange){
								log.info("Only Header/Footer value has changed");
								log.info("Updating existing JSON with new Header/Footer value");
								boolean flag = CommonUtils.updateHeaderandFooterValueInJSON(oldSourceSchema, propertiesMap,srcSystem, country, tableName,excelPropertyMap);
								if(!flag){
									log.error("Fail to update Header/Footer value");
									errorTableList += tableName + ",";
								}			
							}
							//************************************************************************************
							
							else{
								log.info("No new schema changes present in :- " + tableName);									
							}
						} else{
							log.error("Schema version not found for "+ tableName);
							errorTableList += tableName + ",";
						}
					} else {
						RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TEN,
								RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TEN),null,deploymentProperties);
						errorTableList += tableName + ",";
					}
				} else {
					errorTableList += tableName + ",";
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_EIGHTEEN,RemedyLogsUtils.remedyLogsPropertiesMap
							.get(RemedyConstants.PRODUCT_CODE_EIGHTEEN),null,deploymentProperties);
				}
			}
			if (!errorTableList.isEmpty())
				processErrorFile(srcSystem, country, errorTableList,
						hadoopPropertiesMap,countryPropertiesMap);
			
			SchemaEvolution.writeSchemaTableList(newVersionTableList,
					propertiesMap, srcSystem, country,deploymentProperties);

		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());

		}
		RemedyLogsUtils.closeRemedyLogFileHandle();
		return true;
	}
	
	public boolean generate(String srcSystem,String configPath) {

		return true;
	}

	/**
	 * Validates and writes json to hdfs.
	 * Also generates avsc schema, avro and orc hql files using json file
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param schemaInformationObject
	 * @param auditLogsPojoObj
	 * @param schemaVersion
	 * @param orcFlag
	 * @param oldSourceSchema
	 * @param deploymentProperties
	 * @param addedColumnList
	 * @return
	 */
	private boolean generateSchema(String srcSystem, String country,
			Map.Entry<String, TableListStructure> table,
			JSONObject schemaInformationObject,
			Integer schemaVersion, boolean orcFlag, JSONObject oldSourceSchema,Map<String,String> deploymentProperties,List<String> addedColumnList,Integer partition_col_val) {

		String tableName = ParserUtils.cleanString(table.getValue().getName());
		String avscResult = "";
		String avroResult = "";
		String orcResult = "";
		String textResult = "";
		boolean hdfsWriteJson = false;
		boolean hdfsWriteAvsc = false;
		boolean hdfsWriteAvro = false;
		boolean hdfsWriteOrc = false;
		boolean hdfsWriteText = false;
		String retrofit = "";
		try {
			log.info(srcSystem + ":" + country + ":" + tableName + ":"+ "Json is Valid, writing it to HDFS");
			String jsonfilename = srcSystem + "_" + country + "_" + tableName + Constants.JSON_FORMAT;
			JSONObject metaInfo = (JSONObject) schemaInformationObject.get(JSONConstants.METAINFORMATION_SCHEMA);
			
//			System.out.println(schemaInformationObject.toString());
			
			hdfsWriteJson = WriteToHdfs(schemaInformationObject.toString(),jsonfilename, propertiesMap.get(Constants.JSON_OUTPUT_DIR));
			if (!hdfsWriteJson)
				errorTableList += tableName + ",";

			
			//GENERATE STAGING LAYER- TABLES
			// Create AVSC file from JSON
			log.info(srcSystem + ":" + country + ":" + tableName	+ ":JSON to AVSC File converter... ");
			avscResult = GenerateAVSC(srcSystem, country, tableName,schemaInformationObject, dataTypeMappingPropertiesMap,deploymentProperties,addedColumnList,true);
			if (!avscResult.isEmpty()) {
				String avscFilename = srcSystem + "_" + country + "_"+ tableName + Constants.AVSC_FORMAT;
				log.info(srcSystem + ":" + country + ":" + tableName + ":"+ "Created target AVSC schema, writing it to HDFS");
				hdfsWriteAvsc = WriteToHdfs(avscResult, avscFilename,propertiesMap.get(Constants.AVSC_OUTPUT_DIR));
			} else {
				log.error(srcSystem + ":" + country + ":" + tableName + ":Failed to create avsc file");
				RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country, RemedyConstants.PRODUCT_CODE_ELEVEN, RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_ELEVEN), null,deploymentProperties);
			}

			// Create AVRO file from JSON
			if(schemaVersion == 1){
				
				log.info(srcSystem + ":" + country + ":" + tableName + ":Create AVRO DDl from JSON... ");
				avroResult = GenerateAVRO(srcSystem, country, tableName,schemaVersion,true,schemaInformationObject);
				if (!avroResult.isEmpty()) {
					String avroFilename = srcSystem + "_" + country + "_" + tableName+ "_stage" +  Constants.AVRO_FORMAT + Constants.DDL_FORMAT;
					log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive AVRO DDL, writing it to HDFS");
					hdfsWriteAvro = WriteToHdfs(avroResult, avroFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
				} else {
					log.error(srcSystem + ":" + country + ":" + tableName + ":Faile to create avro file");
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country, RemedyConstants.PRODUCT_CODE_TWELVE,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWELVE), null,deploymentProperties);
				 }
			}
//			
//			System.out.println(avroResult);
//			System.out.println("-------------------------------------------");
			String tableFormat = metaInfo.get(JSONConstants.TARGET_DB_FORMAT).toString();
			if(tableFormat.equalsIgnoreCase("orc")){
				// Create ORC DDL from JSON
				log.info(srcSystem + ":" + country + ":" + tableName + ":Create ORC DDL from JSON... ");
				orcResult = GenerateORC(srcSystem, country, tableName,schemaInformationObject, table.getValue().getClassification(), orcFlag, oldSourceSchema,deploymentProperties);
				if (!orcResult.isEmpty()) {
					
					String[] temp = orcResult.split("\\|");
					orcResult = temp[1];
					retrofit = temp[0];
					
					String orcFilename = srcSystem + "_" + country + "_" + tableName + Constants.ORC_FORMAT + "_" + "v" + schemaVersion
							+ Constants.DDL_FORMAT;
					log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive ORC ddl, writing it to HDFS");
					hdfsWriteOrc = WriteToHdfs(orcResult, orcFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
				} else {
					log.error(srcSystem + ":" + country + ":" + tableName
							+ ":Failed to create orc file");
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_THIRTEEN,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_THIRTEEN),null,deploymentProperties);
				}
				
				
				
				// Create TEXTFORMAT DDL FOR TEMP TABLE from JSON - TALEND DATA LOADING
				log.info(srcSystem + ":" + country + ":" + tableName + ":Create TEXT DDL from JSON... ");
				textResult = GenerateTEXT(srcSystem, country, tableName,schemaInformationObject, table.getValue().getClassification(), orcFlag, oldSourceSchema,deploymentProperties,true);
				if (!textResult.isEmpty()) {
					
					String[] temp = textResult.split("\\|");
					textResult = temp[1];
					retrofit = temp[0];
					
					String txtFilename = srcSystem + "_" + country + "_" + tableName + Constants.TEXT_FORMAT + Constants.DDL_FORMAT;
					log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive TEXT ddl, writing it to HDFS");
					hdfsWriteText = WriteToHdfs(textResult, txtFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
				} else {
					log.error(srcSystem + ":" + country + ":" + tableName
							+ ":Failed to create TEXT file");
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_THIRTEEN,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_THIRTEEN),null,deploymentProperties);
				}
//				System.out.println(orcResult);
//				System.out.println(textResult);
//				System.out.println("----------------------------");
			}
			else if(tableFormat.equalsIgnoreCase("textfile"))
			{
				// Create TEXTFORMAT DDL FOR TEMP TABLE from JSON
				log.info(srcSystem + ":" + country + ":" + tableName + ":Create TEXT DDL from JSON... ");
				textResult = GenerateTEXT(srcSystem, country, tableName,schemaInformationObject, table.getValue().getClassification(), orcFlag, oldSourceSchema,deploymentProperties,false);
				if (!textResult.isEmpty()) {
					
					String[] temp = textResult.split("\\|");
					textResult = temp[1];
					retrofit = temp[0];
					
					String txtFilename = srcSystem + "_" + country + "_" + tableName + Constants.TEXT_FORMAT + Constants.DDL_FORMAT;
					log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive TEXT ddl, writing it to HDFS");
					hdfsWriteText = WriteToHdfs(textResult, txtFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
				} else {
					log.error(srcSystem + ":" + country + ":" + tableName
							+ ":Failed to create TEXT file");
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_THIRTEEN,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_THIRTEEN),null,deploymentProperties);
				}
//				System.out.println(textResult);
//				System.out.println("----------------------------");
			}
			else if(tableFormat.equalsIgnoreCase("avro"))
			{
				// Create AVRO file from JSON
				if(schemaVersion == 1){
					
					log.info(srcSystem + ":" + country + ":" + tableName + ":Create AVRO DDl from JSON... ");
					avroResult = GenerateAVRO(srcSystem, country, tableName,schemaVersion,false,schemaInformationObject);
					if (!avroResult.isEmpty()) {
						String avroFilename = srcSystem + "_" + country + "_" + tableName + Constants.AVRO_FORMAT + Constants.DDL_FORMAT;
						log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive AVRO DDL, writing it to HDFS");
						hdfsWriteAvro = WriteToHdfs(avroResult, avroFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
					} else {
						log.error(srcSystem + ":" + country + ":" + tableName + ":Faile to create avro file");
						RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country, RemedyConstants.PRODUCT_CODE_TWELVE,
								RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWELVE), null,deploymentProperties);
					 }
				}
				
				// Create TEXTFORMAT DDL FOR TEMP TABLE from JSON - TALEND DATA LOADING
				log.info(srcSystem + ":" + country + ":" + tableName + ":Create TEXT DDL from JSON... ");
				textResult = GenerateTEXT(srcSystem, country, tableName,schemaInformationObject, table.getValue().getClassification(), orcFlag, oldSourceSchema,deploymentProperties,true);
				if (!textResult.isEmpty()) {
					
					String[] temp = textResult.split("\\|");
					textResult = temp[1];
					retrofit = temp[0];
					
					String txtFilename = srcSystem + "_" + country + "_" + tableName + Constants.TEXT_FORMAT + Constants.DDL_FORMAT;
					log.info(srcSystem + ":" + country + ":" + tableName + ":" + "Created target Hive TEXT ddl, writing it to HDFS");
					hdfsWriteText = WriteToHdfs(textResult, txtFilename,propertiesMap.get(Constants.DDL_OUTPUT_DIR));
				} else {
					log.error(srcSystem + ":" + country + ":" + tableName
							+ ":Failed to create TEXT file");
					RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_THIRTEEN,
							RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_THIRTEEN),null,deploymentProperties);
				}
//				System.out.println(avroResult);
//				System.out.println(textResult);
//				System.out.println("----------------------------");
			}
		
			// Write to audit logs
//			AuditClient.writeToAuditLog(schemaInformationObject, avscResult, avroResult,orcResult, hdfsWriteAvro, hdfsWriteAvsc, hdfsWriteJson,
//					hdfsWriteOrc, auditLogsPojoObj, srcSystem, country,basicInfo,hadoopPropertiesMap,auditHandler,auditTableName,serverIP,deploymentProperties);

		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
		if(retrofit.equalsIgnoreCase("yes"))			
			return true;
		else
			return false;
	}
		
	/**
	 * generate json file using schema file
	 * @param inputDataType
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param ddlschemaFileList
	 * @param metaInformation
	 * @param datatypeMap
	 * @param workbook
	 * @param excelPropertyMap
	 * @param classification
	 * @return
	 */
	private JSONObject GenerateJSON(String inputDataType, String srcSystem,String country, String table, ArrayList<String> ddlschemaFileList,JSONObject metaInformation, Map<String, String> datatypeMap,
			Workbook workbook, Map<String, String> excelPropertyMap,String classification) {
		IParser Parser = ParserFactory.createParser(inputDataType);
		JSONObject schemaInformationObject = Parser.createJsonfromSchema(ddlschemaFileList, metaInformation, propertiesMap, srcSystem,country, table, datatypeMap, workbook, excelPropertyMap,classification);
		return schemaInformationObject;
	}

	/**
	 * generate AVSC String
	 * 
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param schemaInformationObject
	 * @param datatypeMap
	 * @param deploymentProperties
	 * @param addedColumnList
	 * @return
	 */
	private String GenerateAVSC(String srcSystem, String country, String table,JSONObject schemaInformationObject, Map<String, String> datatypeMap, Map<String,String> deploymentProperties,List<String> addedColumnList,boolean is_dynamic_part) {
		AvscConverter avscConverter = new AvscConverter();
		String result = avscConverter.convert(schemaInformationObject,propertiesMap, srcSystem, country, table, datatypeMap,addedColumnList,is_dynamic_part);
		return result;
	}

	/**
	 * generate ORC query String
	 * 
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param schemaInformationObject
	 * @param classification
	 * @param orcFlag
	 * @param oldSourceSchema
	 * @param deploymentProperties
	 * @return
	 */
	private String GenerateORC(String srcSystem, String country, String table,JSONObject schemaInformationObject, String classification,boolean orcFlag, JSONObject oldSourceSchema, Map<String,String> deploymentProperties) {
		String result = "";
		if (orcFlag)
			result = HiveORCSchemaEvolution.convert(schemaInformationObject,propertiesMap, srcSystem, country, table, oldSourceSchema,dataTypeMappingPropertiesMap,deploymentProperties);
		else
			result = HiveORCConverter.convert(schemaInformationObject,propertiesMap, srcSystem, country, table);

		return result;
	}
	
	/**
	 * generate TEXT query String
	 * 
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param schemaInformationObject
	 * @param classification
	 * @param orcFlag
	 * @param oldSourceSchema
	 * @param deploymentProperties
	 * @return
	 */
	private String GenerateTEXT(String srcSystem, String country, String table,JSONObject schemaInformationObject, String classification,boolean orcFlag, JSONObject oldSourceSchema, Map<String,String> deploymentProperties,boolean is_temp) {
		String result = "";
		result = TextConverter.convert(schemaInformationObject,propertiesMap, srcSystem, country, table,is_temp);

		return result;
	}

	/**
	 * generate AVRO String
	 * 
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param classification
	 * @param schemaVersion
	 * @return
	 */
	private String GenerateAVRO(String srcSystem, String country, String table,Integer schemaVersion,boolean is_staging,JSONObject schemaInformationObject) {
		String result = HiveAvroConverter.convert(propertiesMap, srcSystem,country, table, schemaVersion,is_staging,schemaInformationObject);
		return result;
	}

	/**
	 * write json to HDFS String
	 * 
	 * @param outPutData
	 * @param filename
	 * @param filepath
	 * @throws HDFSFileOperationException
	 * @throws IOException
	 */
	private boolean WriteToHdfs(String outPutData, String filename,String filepath) throws IOException, HDFSFileOperationException {
		HDFSHandle hdfsHandler = new HDFSHandle();
		boolean result = hdfsHandler.hdfsWrite(outPutData, propertiesMap,filename, filepath);
		if (result) {
			log.info("file created at " + filepath + "/" + filename);
		} else {
			return false;
		}
		return true;
	}

	/**
	 * Process error file
	 * @param srcSystem
	 * @param country
	 * @param errorTableList
	 * @param hadoopPropertiesMap
	 * @param countryMap
	 */
	private void processErrorFile(String srcSystem, String country,String errorTableList, Map<String, String> hadoopPropertiesMap,Map<String,String> countryMap) {
		if(!inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE))
			CommonUtils.writeErrorFile(srcSystem, country, errorTableList,hadoopPropertiesMap,countryMap);
		log.error("Error occure while processing following tables:- "+ errorTableList);
	}
	
	
	/**
	 * Initialize all property maps
	 * @param srcSystem
	 * @param country
	 * @param config
	 * @param property
	 */
	private void initialization(String srcSystem,String country,ConfigUtils config,Properties property){
		deploymentProperties = config.readPropertyFile(srcSystem,country,property.getProperty(srcSystem.toUpperCase() + "_" + country.toUpperCase() + Constants.DEPLOYMENT_PROPERTIES_PATH));
		RemedyLogsUtils.createMapProductErrorCode(srcSystem,country,deploymentProperties.get(RemedyConstants.REMEDY_PRODUCT_CODE_PATH));
		hadoopPropertiesMap = config.readPropertyFile(srcSystem, country,property.getProperty(Constants.HADOOP_CONFIGURATION_PATH));
		dataTypeMappingPropertiesMap = config.readPropertyFile(srcSystem,country,property.getProperty(srcSystem.toUpperCase()+ Constants.DATATYPE_MAPPING_PATH));
		sourcePropertiesMap = config.readPropertyFile(srcSystem,country,property.getProperty(srcSystem.toUpperCase() + Constants.PROPERTIES_PATH));
		countryPropertiesMap = config.readPropertyFile(srcSystem,country,property.getProperty(srcSystem.toUpperCase() + "_" + country.toUpperCase() + Constants.COUNTRY_PROPERTY_PATH));
//		serverIP = (String) hadoopPropertiesMap.get(AuditConstants.HBASE_SERVER_IP);
//		auditTableName = (String) hadoopPropertiesMap.get(AuditConstants.HBASE_TABLE_NAME);
//		log.info("\n" + srcSystem + ":" + country + ": Hbase  serverIP" + serverIP + "Hbase auditTableName " + auditTableName);
		inputDataType = sourcePropertiesMap.get(Constants.SOURCE_INPUT_TYPE);			
		log.info("Source type for current system is " + inputDataType);
	}

	/**
	 * Create table list for generator
	 * @param srcSystem
	 * @param country
	 * @param config
	 * @param property
	 * @return
	 */
	private Map<String, TableListStructure> createTableListGenerator(String srcSystem,String country,ConfigUtils config,Properties property){
		Map<String, TableListStructure> tableListMap = new HashMap<String, TableListStructure>();
		if (inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE)) {
			workbook = config.readExcelFile(property.getProperty(srcSystem.toUpperCase()+ "_"+ country.toUpperCase()+ Constants.INPUT_FILE_PATH));
			tableListMap = config.createExcelTableList(workbook);
		} else if(inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE)){
			tableListMap = config.createTableListFromDB(sourcePropertiesMap,srcSystem);
		} else {
			workbook = config.readExcelFile(property.getProperty(srcSystem.toUpperCase()+ "_"+ country.toUpperCase()+ Constants.TABLE_LIST_PATH));
			tableListMap = config.createTableList(workbook);
		}
		return tableListMap;
	}
	
	/**
	 * Check property values for NULL
	 * @param srcSystem
	 * @param country
	 * @param config
	 * @param property
	 */
	private void propertyCheck(String srcSystem,String country,ConfigUtils config,Properties property){

		if (sourcePropertiesMap.get(Constants.IS_SCHEMA_EVOLUTION) == null) {
			String errorMessage = "Define schema evolution flag in source property file for " + srcSystem;
			log.error(errorMessage);
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country, RemedyConstants.PRODUCT_CODE_SIX, RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_SIX),
					errorMessage,deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}
		if (countryPropertiesMap.get(Constants.IS_SCHEMA_EVOLUTION) == null) {
			String errorMessage = "Define schema evolution flag in country property file for " + country;

			log.error(errorMessage);
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_SIX,RemedyLogsUtils.remedyLogsPropertiesMap
							.get(RemedyConstants.PRODUCT_CODE_SIX),errorMessage,deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}if(dataTypeMappingPropertiesMap.get(Constants.AVRO_INCOMPATIBLE_TYPES) == null){
			String errorMessage = "Incompatable types are not Define in Data Mapping File";

			log.error(errorMessage);
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT, srcSystem, country,RemedyConstants.PRODUCT_CODE_TWENTYFOUR,RemedyLogsUtils.remedyLogsPropertiesMap
							.get(RemedyConstants.PRODUCT_CODE_TWENTYFOUR),errorMessage,deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}	
	}
	
	/**
	 * Create recon table JSON and HQL file
	 * @param tableName
	 * @param country
	 * @param srcSystem
	 * @param schemaInformationObject
	 */
	private void createReconTableGenerator(String tableName,String country,String srcSystem,JSONObject schemaInformationObject){
		log.info(":::::::recon table " + tableName);
		ReconTableCreation recon = new ReconTableCreation();
		log.info("creating JSON file for recon table :- " + tableName);
		String jsonfilename = srcSystem + "_" + country + "_" + tableName + Constants.JSON_FORMAT;
		boolean writeJsonToHdfs = recon.createReconJsonFile(jsonfilename,schemaInformationObject.toString(),propertiesMap);
		if(!writeJsonToHdfs){
			log.error("Fail to create JSON for recon table :- " + tableName);
			errorTableList += tableName + ",";
		}
		log.info("creating HQL file for recon table :- " + tableName);
		String orcFilename = srcSystem + "_" + country + "_" + tableName + Constants.DDL_FORMAT;
		boolean writeReconHqltoHdfs = recon.createReconHqlFile(orcFilename,schemaInformationObject,propertiesMap,srcSystem,country,tableName,deploymentProperties);
		if(!writeReconHqltoHdfs){
			log.error("Fail to create HQL for recon table :- " + tableName);
			errorTableList += tableName + ",";
		}		
	}
	
	/**
	 * Read schema file and generate JSON object
	 * @param srcSystem
	 * @param country
	 * @param tableName
	 * @param classification
	 * @param metaInformation
	 * @param schemaFile
	 * @param excelPropertyMap
	 * @return
	 */
	private JSONObject readSchemaFileGenerator(String srcSystem,String country,String tableName,String classification,JSONObject metaInformation,FileReader schemaFile,Map<String,String> excelPropertyMap){
		JSONObject schemaInformationObject = null;
		ArrayList<String> schemaFileData = new ArrayList<String>();
		
		if (inputDataType.equalsIgnoreCase(Constants.EXCEL_SOURCE)) {
			// Parse schema and create JSON
			log.info(srcSystem + ":" + country + ":" + tableName + ":parse meta and creating json... ");
			schemaInformationObject = GenerateJSON(inputDataType, srcSystem, country, tableName, null, metaInformation, dataTypeMappingPropertiesMap,
					workbook, excelPropertyMap,classification);
		} else if(inputDataType.equalsIgnoreCase(Constants.RDBMS_SOURCE)){
			log.info(srcSystem + ":" + country + ":" + tableName + ":parse meta and creating json... ");
			schemaInformationObject = GenerateJSON(inputDataType, srcSystem, country, tableName, null, metaInformation, dataTypeMappingPropertiesMap,
					null, null,classification);
		} else {
			// Parse schema data depending on input source type and create JSON
			ISchemaReader schemaFileReader = FileReaderFactory.createSchemaReader(inputDataType);
			schemaFileData = schemaFileReader.scanSchemaFile(schemaFile);
			if (!schemaFileData.isEmpty()) {
				// parse schema and create JSON
				log.info(srcSystem + ":" + country + ":" + tableName + ":parse meta and creating json... ");
				schemaInformationObject = GenerateJSON(inputDataType, srcSystem, country, tableName, schemaFileData, metaInformation, dataTypeMappingPropertiesMap, null, null,classification);
			} else {
				errorTableList += tableName + ",";
			}
		}
		
		return schemaInformationObject;
	}
	
}
