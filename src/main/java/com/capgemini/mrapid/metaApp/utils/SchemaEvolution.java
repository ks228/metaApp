package com.capgemini.mrapid.metaApp.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.MetaInfoConstants;
import com.capgemini.mrapid.metaApp.constants.RemedyConstants;
import com.capgemini.mrapid.metaApp.exceptions.HDFSFileOperationException;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.integration.impl.HiveHandle;
import com.capgemini.mrapid.metaApp.metadata.impl.MetaDataUtils;
import com.capgemini.mrapid.metaApp.pojo.DefaultAvroStructure;
import com.capgemini.mrapid.metaApp.pojo.HiveColumnStructure;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * Class contains schema evolution methods
 * @author Anurag Udasi
 * Utility class for schema evolution
 */

public class SchemaEvolution {

	final static Logger log = Logger.getLogger(SchemaEvolution.class);
	
	/**
	 * getSchemaVersionMap : gives hash map for table and table version 
	 * @param hadoopPropertiesMap
	 * @param srcSystem
	 * @param country
	 * @param deploymentProperties
	 * @return
	 */
	public static Map<String,Integer> getSchemaVersionMap(Map<String,String> hadoopPropertiesMap,String srcSystem,String country,Map<String,String> deploymentProperties){
		
		List<String> queryParameters = new ArrayList<String>();
		List<String> selectParameters = new ArrayList<String>();
		HiveHandle hiveHandler = new HiveHandle();
		MetaDataUtils metaDataUtils = new MetaDataUtils();
		Map<String,Integer> schemaVersion = new HashMap<String,Integer>();
		String db_name =  deploymentProperties.get(Constants.METADATA_DBNAME);
		
		try{
			String statement = "table_name,max(version)";
			queryParameters.add("source");
			queryParameters.add("country_name");
			String groupByStmt = "group by table_name";
			String query = metaDataUtils.constructSelectQuery(db_name,srcSystem,Constants.METADATA_ALL_COLUMNS,queryParameters,statement,groupByStmt);
			selectParameters.add(srcSystem.toUpperCase());
			selectParameters.add(country.toUpperCase());
			schemaVersion = hiveHandler.selectMaxVersionTableMap(hadoopPropertiesMap,query,selectParameters,srcSystem,country,deploymentProperties);
		}catch(Exception e){
			log.error("Not able to established Hive connection / Check common tables" + e.getMessage());
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYFIVE,
					RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYFIVE),"Hive connection problem / Check common tables",deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}
		
		return schemaVersion;
	}
	
	
	/**
	 * getSchemaCntryVersion : Gives current country latest schema version
	 * @param hadoopPropertiesMap
	 * @param srcSystem
	 * @param country
	 * @param deploymentProperties
	 * @return
	 */
	public static Integer getSchemaCntryVersion(Map<String,String> hadoopPropertiesMap,String srcSystem,String country,Map<String,String> deploymentProperties){
		
		List<String> queryParameters = new ArrayList<String>();
		HiveHandle hiveHandler = new HiveHandle();
		MetaDataUtils metaDataUtils = new MetaDataUtils();
		Integer schemaVersion = 0;
		String db_name =  deploymentProperties.get(Constants.METADATA_DBNAME);
		
		try{
			String statement = "max(version)";
			queryParameters.add("source");
			queryParameters.add("country_name");
			String query = metaDataUtils.constructSelectQuery(db_name,srcSystem,Constants.METADATA_ALL_COUNTRY,queryParameters,statement,null);
			schemaVersion = Integer.parseInt(hiveHandler.selectMaxVersion(hadoopPropertiesMap,query,country.toUpperCase(),srcSystem.toUpperCase(),hadoopPropertiesMap.get(Constants.HDFS_URL)));
			log.info("Latest version for " + country  + " is " + schemaVersion);
		}catch(Exception e){
			log.error("Not able to established Hive connection / Check common tables" + e.getMessage());
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYFIVE,
					RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYFIVE),"Hive connection problem / Check common tables",deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}
		
		return schemaVersion;
	}
	
	
	/**
	 * Gets old json schema from hdfs path specified
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param propertiesMap
	 * @param version
	 * @return
	 */
	public static JSONObject getOldSourceSchema(String srcSystem,String country,String table,Map<String,String> propertiesMap,Integer version){
		
		HDFSHandle hdfsHandleobj = new HDFSHandle();
		JSONObject jsonObj = null;
		try{
			log.info("reading old schema file");
			String hdfsPath = propertiesMap.get(Constants.JSON_OUTPUT_DIR) + "/" +
					srcSystem + "_" + country + "_" + table + Constants.JSON_FORMAT;

			log.info("Read old schema from " + hdfsPath);
			
			String jsonString = hdfsHandleobj.hdfsRead(hdfsPath.toLowerCase(),
					propertiesMap.get(Constants.HDFS_URL));
			if(!jsonString.isEmpty())
				jsonObj = new JSONObject(jsonString);
			else{
				return null;
			}
		}catch(JSONException e){
			log.error(e.getMessage());
		} catch (IOException e) {
			log.error(e.getMessage());
		}
		return jsonObj;
	}
	
	/**
	 * Compare old and new json schema
	 * @param oldSchema
	 * @param newSchema
	 * @return
	 */
	public static boolean compareSourceSchema(JSONObject oldSchema,JSONObject newSchema){
		Gson g = new Gson();
		boolean flag = false;
		
		try{
		String oldSourceSchema = oldSchema.getJSONObject(JSONConstants.SOURCE_SCHEMA).toString();
		String newSourceSchema = newSchema.getJSONObject(JSONConstants.SOURCE_SCHEMA).toString();
		
		JsonElement element1 = g.fromJson(oldSourceSchema, JsonElement.class);
		JsonElement element2 = g.fromJson(newSourceSchema, JsonElement.class);
		
		flag = compareJson(element1, element2);
		}catch(JSONException e)
		{
			log.error(e.getMessage());
		}
		return flag;
	}
	
	
	
	/**
	 * Takes old and new schema JSON and iterate through each element to get differensce
	 * @param json1
	 * @param json2
	 * @return
	 */
	public static boolean compareJson(JsonElement json1, JsonElement json2) {
		boolean isEqual = true;
		// Check whether both jsonElement are not null
		if(json1 !=null && json2 !=null) {        
			// Check whether both jsonElement are objects
			if (json1.isJsonObject() && json2.isJsonObject()) {
				Set<Entry<String, JsonElement>> ens1 = ((JsonObject) json1).entrySet();
				Set<Entry<String, JsonElement>> ens2 = ((JsonObject) json2).entrySet();
				JsonObject json2obj = (JsonObject) json2;
				if (ens1 != null && ens2 != null && (ens2.size() == ens1.size())) {
					// Iterate JSON Elements with Key values
					for (Entry<String, JsonElement> en : ens1) {
						isEqual = isEqual && compareJson(en.getValue() , json2obj.get(en.getKey()));
					}
				} else {
					return false;
				}
			}     
			// Check whether both jsonElement are arrays
			else if (json1.isJsonArray() && json2.isJsonArray()) {
				JsonArray jarr1 = json1.getAsJsonArray();
				JsonArray jarr2 = json2.getAsJsonArray();
				if(jarr1.size() != jarr2.size()) {
					return false;
				} else {
					int i = 0;
					// Iterate JSON Array to JSON Elements
					for (JsonElement je : jarr1) {
						isEqual = isEqual && compareJson(je , jarr2.get(i));
						i++;
					}   
				}
			}    
			// Check whether both jsonElement are null
			else if (json1.isJsonNull() && json2.isJsonNull()) {
				return true;
			}     
			// Check whether both jsonElement are primitives
			else if (json1.isJsonPrimitive() && json2.isJsonPrimitive()) {
				if(json1.equals(json2)) {
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		} else if(json1 == null && json2 == null) {
			return true;
		} else {
			return false;
		}
		return isEqual;
	}	
	
	/**
	 * Move old ORC and AVRO files to archive
	 * @param schemaVersion
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param tableName
	 */
	public static void moveOldSchema(Integer schemaVersion,Map<String,String> propertiesMap,String srcSystem,String country,String tableName){
		HDFSHandle hdfsHandler = new HDFSHandle();
		String fileName =  srcSystem + "_" + country + "_" + tableName;
		int finalVersion = schemaVersion-1;
		String version = "_" + "v";
		String tmpFilePath = propertiesMap.get(Constants.LOCAL_BASE_PATH)+ "/" + srcSystem.toLowerCase();
		
		try{
		String ddlArchivePath = propertiesMap.get(Constants.DDL_OUTPUT_DIR);
		if(finalVersion ==1) 
			hdfsHandler.hdfsMoveFile(ddlArchivePath, propertiesMap.get(Constants.HDFS_ARCHIVE_PATH),fileName+ Constants.AVRO_FORMAT ,fileName + Constants.AVRO_FORMAT + version + finalVersion, propertiesMap.get(Constants.HDFS_URL), Constants.DDL_FORMAT,tmpFilePath,true);
		
		hdfsHandler.hdfsMoveFile(ddlArchivePath, propertiesMap.get(Constants.HDFS_ARCHIVE_PATH),fileName + Constants.ORC_FORMAT + version + finalVersion,fileName + Constants.ORC_FORMAT + version + finalVersion,propertiesMap.get(Constants.HDFS_URL), Constants.DDL_FORMAT,tmpFilePath,true);
		
		
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}	
	/**
	 * Move old JSON and AVSC file to archive
	 * @param schemaVersion
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param tableName
	 */
	public static void moveOldJSONFile(Integer schemaVersion,Map<String,String> propertiesMap,String srcSystem,String country,String tableName){
		HDFSHandle hdfsHandler = new HDFSHandle();
		String fileName =  srcSystem + "_" + country + "_" + tableName;
		int finalVersion = schemaVersion-1;
		String version = "_" + "v";
		String tmpFilePath = propertiesMap.get(Constants.LOCAL_BASE_PATH) + "/" + srcSystem.toLowerCase();
		try{
			String jsonArchivePath = propertiesMap.get(Constants.JSON_OUTPUT_DIR);
			String avroArchivePath = propertiesMap.get(Constants.AVSC_OUTPUT_DIR);
			
			hdfsHandler.hdfsMoveFile(jsonArchivePath, propertiesMap.get(Constants.HDFS_ARCHIVE_PATH),fileName ,fileName + version + finalVersion, propertiesMap.get(Constants.HDFS_URL), Constants.JSON_FORMAT,tmpFilePath,true);
			hdfsHandler.hdfsMoveFile(avroArchivePath, propertiesMap.get(Constants.HDFS_ARCHIVE_PATH),fileName, fileName + version + finalVersion,propertiesMap.get(Constants.HDFS_URL), Constants.AVSC_FORMAT,tmpFilePath,true);
			
		}catch(Exception e){
				log.error(e.getMessage());
		}		
	}
	
	/**
	 * Create ORC retrofit query
	 * @param propertiesMap
	 * @param jsonDestinationschemaArray
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @return
	 */
	public static String createORCRetroFitQuery(Map<String,String> propertiesMap,JSONArray jsonDestinationschemaArray,String srcSystem,String country,String table){

		StringBuilder query = new StringBuilder();
		String partiton_col = propertiesMap.get(Constants.TABLE_PARTITION);
		try{
		query.append("DROP TABLE IF EXISTS " + propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + "_temp;" + "\n");	
		query.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + "_temp" + " ( \n");
		query.append(ParserUtils.createORCtableSchemaQuery(jsonDestinationschemaArray,partiton_col));
		query.append(") \n");
		query.append("PARTITIONED BY ("+ partiton_col + " " + Constants.PARTITION_TYPE + ")\n");
		query.append("ROW FORMAT \nSERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES(\"serialization.encoding\"='" + propertiesMap.get(Constants.ENCODINGCHARSET)  + "',\"field.delim\"='" + propertiesMap.get(Constants.TARGET_DELIMETED) +"',\"serialization.format\"='"+ propertiesMap.get(Constants.TARGET_DELIMETED) +"',\"orc.compression\"='"+ propertiesMap.get(Constants.ORC_COMPRESSION)+ "')\n");
		query.append("STORED AS " + propertiesMap.get(Constants.TARGET_DB_FORMAT)  +"\n");
		query.append("LOCATION '" +  propertiesMap.get(Constants.HIVE_BASE_PATH_PROCESSING) + "_temp"+ "'; \n");
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return query.toString();
	}
	
	
	/**
	 * Create default column list
	 * @param oldSchemaMap
	 * @return
	 */
	public static Map<String,DefaultAvroStructure> getDefaultColumnList(Map<String,HiveColumnStructure> oldSchemaMap){
		Map<String,DefaultAvroStructure> colList = new HashMap<String,DefaultAvroStructure>();
		for (Map.Entry<String, HiveColumnStructure> entry : oldSchemaMap.entrySet())
		{
			DefaultAvroStructure avroStructure = new DefaultAvroStructure();
			if(entry.getValue().getDefault() != null ){
				avroStructure.setColumnname(ParserUtils.cleanString(entry.getKey()));
				avroStructure.setDatatype(entry.getValue().getColtype());
				avroStructure.setDefault(entry.getValue().getDefault());
				colList.put(ParserUtils.cleanString(entry.getKey()),avroStructure);
			}	
		}
		
		return colList;
	}
	
	
	/**
	 * Update JSON with default values, compatible data types etc.
	 * @param oldSchema
	 * @param newSchema
	 * @param incompatibleTypes
	 * @return
	 */
	public static JSONObject getUpdatedJson(JSONObject oldSchema, JSONObject newSchema,String incompatibleTypes){
		Map<String,DefaultAvroStructure> colList = new HashMap<String,DefaultAvroStructure>();
		Map<String,DefaultAvroStructure> colListtemp = new HashMap<String,DefaultAvroStructure>();
		ArrayList<String> colTypeChange = new ArrayList<String>();
		JSONObject updatedJSONSchema = new JSONObject();
		try{
			JSONObject oldDescSchema = oldSchema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
			JSONArray oldDescArray = oldDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
			Map<String,HiveColumnStructure> oldDescMap = SchemaEvolution.getSchemaMap(oldDescArray);
			log.info("check for incompatible types");
			Map<String,String> incompatibleType = SchemaEvolution.readIncompatibleTypeValues(incompatibleTypes); 
			boolean incompatibaleJSON = SchemaEvolution.getIncompatibleCol(oldSchema, newSchema, incompatibleType);
			if(incompatibaleJSON){
				log.info("incompatable data type change is present in JSON");
				return null;
			}
			log.info("Get old default column");
			colList = SchemaEvolution.getDefaultColumnList(oldDescMap); 
			log.info("Get new added column");
			colListtemp = SchemaEvolution.getNewColumnList(oldSchema, newSchema);
			for(Map.Entry<String, DefaultAvroStructure> entry : colListtemp.entrySet()){
				colList.put(entry.getKey(),entry.getValue());
			}
			
			log.info("Update Json with default Values");
			updatedJSONSchema = SchemaEvolution.updateJSON(colList,newSchema,colTypeChange);						
			
		}catch(Exception e){
			log.info(e.getMessage());
		}	
		return updatedJSONSchema;		
	}
	
	/**
	 * Read incompatible data types
	 * @param incompatibleTypes
	 * @return
	 */
	public static Map<String, String> readIncompatibleTypeValues (String incompatibleTypes) {
		Map<String, String> incompatibleTypesMap = new HashMap<String, String>();

		String[] incompatibleTypePairs = incompatibleTypes.split("\\|");
		for ( String value : incompatibleTypePairs)
		{
			incompatibleTypesMap.put(value,Constants.INCOMPATIBLE_TYEP_VALUE);
		}

		return incompatibleTypesMap;


	}
	
	/**
	 * Get incompatible column
	 * @param oldSchema
	 * @param newSchema
	 * @param incompatibleType
	 * @return
	 */
	public static boolean getIncompatibleCol(JSONObject oldSchema,JSONObject newSchema,Map<String,String> incompatibleType){
		try{
		JSONObject oldDescSchema = oldSchema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
		JSONObject newDescSchema = newSchema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
		
		JSONArray oldSourceArray = oldDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
		JSONArray newSourceArray = newDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
		
		Map<String,HiveColumnStructure> oldSchemaMap = SchemaEvolution.getSchemaMap(oldSourceArray);
		Map<String,HiveColumnStructure> newSchemaMap = SchemaEvolution.getSchemaMap(newSourceArray);
		for (Map.Entry<String, HiveColumnStructure> newEntry : newSchemaMap.entrySet())
		{
			boolean flag = false;
		   for(Map.Entry<String, HiveColumnStructure> oldEntry : oldSchemaMap.entrySet()){
			   if(newEntry.getKey().toString().equalsIgnoreCase(oldEntry.getKey().toString())){
				   String oldType = oldEntry.getValue().getColtype();
				   String newType = newEntry.getValue().getColtype();
				   String key = oldType + "-" + newType;
				   if(incompatibleType.containsKey(key)){
					   flag = true;
				   }
				   break;					
			    }   
		   }
		   if(flag)
			   return true;
		}
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return false;		
	}
	
	/**
	 * Update JSON by adding default values
	 * @param columnList
	 * @param schema
	 * @param typeColList
	 * @return
	 */
	public static JSONObject updateJSON(Map<String,DefaultAvroStructure> columnList,JSONObject schema,ArrayList<String> typeColList){
	
		JSONObject finalJsonSchema = new JSONObject();
		try{
		JSONObject SourceSchema = schema.getJSONObject(JSONConstants.SOURCE_SCHEMA);
		JSONObject descSchema = schema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
		JSONObject transSchem = schema.getJSONObject(JSONConstants.TRANSFORMATION_SCHEMA);
		JSONObject metaInfoSchema = schema.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
		JSONArray descColumnArray = descSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
		JSONArray newSourceColumnArray = new JSONArray();
		for(int i = 0;i<descColumnArray.length();i++){
			JSONObject defaultDescColumnJson = new JSONObject();
			defaultDescColumnJson = descColumnArray.getJSONObject(i);
			if(typeColList.size() > 0){
				for(int j = 0; j< typeColList.size();j++){
					if(typeColList.get(j).equalsIgnoreCase(defaultDescColumnJson.getString(JSONConstants.COLUMN_NAME))){
						defaultDescColumnJson.put(JSONConstants.COLUMN_TYPE,"string");
						if (defaultDescColumnJson.has(JSONConstants.COLUMN_DEFAULT))							
							defaultDescColumnJson.put(JSONConstants.COLUMN_DEFAULT,"");
					}
				}
			}
			if(columnList.size() > 0){
				for(Map.Entry<String, DefaultAvroStructure> entry : columnList.entrySet()){
					if(entry.getKey().equalsIgnoreCase(defaultDescColumnJson.getString(JSONConstants.COLUMN_NAME))){
						if(entry.getValue().getDefault() != null ){
							if(defaultDescColumnJson.get("type").toString().equalsIgnoreCase("string") || defaultDescColumnJson.get("type").toString().equalsIgnoreCase("byte"))
								defaultDescColumnJson.put(JSONConstants.COLUMN_DEFAULT,"");
							else
								defaultDescColumnJson.put(JSONConstants.COLUMN_DEFAULT,"null");
							}else{
							if(entry.getValue().getDatatype().equalsIgnoreCase("string") || entry.getValue().getDatatype().equalsIgnoreCase("byte")){
								defaultDescColumnJson.put(JSONConstants.COLUMN_DEFAULT,"");
							}else{
								defaultDescColumnJson.put(JSONConstants.COLUMN_DEFAULT,"null");
							}
						}
					}
				}
			}
			newSourceColumnArray.put(defaultDescColumnJson);
		}
		descSchema.put(JSONConstants.COLUMNS_ARRAY, newSourceColumnArray);
		
		finalJsonSchema.put(JSONConstants.SOURCE_SCHEMA, SourceSchema);
		finalJsonSchema.put(JSONConstants.DESTINATION_SCHEMA,
				descSchema);
		finalJsonSchema.put(JSONConstants.TRANSFORMATION_SCHEMA,
				transSchem);
		finalJsonSchema.put(JSONConstants.METAINFORMATION_SCHEMA,
				metaInfoSchema);
		
		}catch(Exception e){
			log.info(e.getMessage());
		}
				
		return finalJsonSchema;
		
	}
	
	/**
	 * 
	 * @param tableList
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param deploymentMap
	 * @throws IOException
	 * @throws HDFSFileOperationException
	 */
	public static void writeSchemaTableList(StringBuilder tableList,Map<String,String> propertiesMap,String srcSystem,String country,Map<String,String> deploymentMap) throws IOException, HDFSFileOperationException{
		log.info("creating schema evolution file");
		String fileName = srcSystem + "_" + country + ".tablelist";
		String filepath = deploymentMap.get(Constants.SCHEMA_EVAL_LOG_FILE_PATH);
		HDFSHandle hdfsHandler = new HDFSHandle();
		boolean result = hdfsHandler.hdfsWrite(tableList.toString() ,propertiesMap, fileName,filepath);
		if(result){
			log.info("Schema file created at " + deploymentMap.get(Constants.SCHEMA_EVAL_LOG_FILE_PATH));
		}else{
			log.error("Fail to create schema file");
			System.exit(1);
		}	
	}
	
	/**
	 * Get newly added column from new schema
	 * @param oldSchema
	 * @param newSchema
	 * @return
	 */
	public static Map<String,DefaultAvroStructure> getNewColumnList(JSONObject oldSchema,JSONObject newSchema){
		Map<String,DefaultAvroStructure> columnList = new HashMap<String,DefaultAvroStructure>();
		try{
			JSONObject oldDescSchema = oldSchema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
			JSONObject newDescSchema = newSchema.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
			JSONArray oldSourceArray = oldDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
			JSONArray newSourceArray = newDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
			Map<String,HiveColumnStructure> oldSchemaMap = SchemaEvolution.getSchemaMap(oldSourceArray);
			Map<String,HiveColumnStructure> newSchemaMap = SchemaEvolution.getSchemaMap(newSourceArray);
			Map<String,HiveColumnStructure> finalSchemaMap = new HashMap<String,HiveColumnStructure>();
			
			for (Map.Entry<String, HiveColumnStructure> newEntry : newSchemaMap.entrySet())
			{
				boolean flag = true;
			   for(Map.Entry<String, HiveColumnStructure> oldEntry : oldSchemaMap.entrySet()){
				   if(newEntry.getKey().toString().equalsIgnoreCase(oldEntry.getKey().toString())){
					   flag = false;
					   break;
				    }   
			   }
			   if(flag)
				   finalSchemaMap.put(newEntry.getKey(), newEntry.getValue());
			}
			if(finalSchemaMap.size() > 0){
				for (Map.Entry<String, HiveColumnStructure> entry : finalSchemaMap.entrySet())
				{
					DefaultAvroStructure avroStructure = new DefaultAvroStructure();
					avroStructure.setColumnname(ParserUtils.cleanString(entry.getKey()));
					avroStructure.setDatatype(entry.getValue().getColtype());
					avroStructure.setDefault(entry.getValue().getDefault());
					columnList.put(ParserUtils.cleanString(entry.getValue().getColname()),avroStructure); 
				}
			}
		}catch(Exception e){
			log.error(e.getMessage());
		}
		
		return columnList;
	}
	
	
	/**
	 * Create schema evolution table list
	 * @param tableName
	 * @param classification
	 * @param version
	 * @param isRetrofit
	 * @return
	 */
	public static String createSchemaTableList(String tableName,String classification,Integer version,boolean isRetrofit){
		String retroFit = "";
		String schemaTableList = "";
		
		if(isRetrofit)
			retroFit = "Y";
		else
			retroFit = "N";
			
		schemaTableList = tableName + "|" + classification + "|" + version + "|" + retroFit + "\n";

		return schemaTableList;
		
	}
	
	
	/**
	 * Get schema Map
	 * @param schemaArray
	 * @return
	 * @throws JsonParseException
	 * @throws JSONException
	 */
	private static Map<String,HiveColumnStructure> getSchemaMap(JSONArray schemaArray) throws JsonParseException, JSONException{
		Map<String,HiveColumnStructure> schemaMap = new HashMap<String, HiveColumnStructure>();
		Gson gson = new Gson();
		for(int i =0;i < schemaArray.length();i++){
			HiveColumnStructure columnInfo = gson.fromJson(schemaArray.get(i).toString(), HiveColumnStructure.class); 
			schemaMap.put(columnInfo.getColname(), columnInfo);
		}
		return schemaMap;
	}
	
	/**
	 * Compare old and new threshold value
	 * @param oldSchema
	 * @param newSchema
	 * @return
	 */
	public static boolean compareThresholdValues(JSONObject oldSchema,String threshold){
		try{
			JSONObject metaInfo = oldSchema.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
			if(metaInfo.has(MetaInfoConstants.THRESHOLD_LIMIT)){
				String thresholdVal = metaInfo.getString(MetaInfoConstants.THRESHOLD_LIMIT);
				if(thresholdVal != null && !thresholdVal.isEmpty() && !thresholdVal.equalsIgnoreCase("")){
					if(!thresholdVal.equalsIgnoreCase(threshold))
						return false;
				}else{
					return false;
				}
			}else{
				return false;
			}	
		}catch(JSONException e)
		{
			log.error(e.getMessage());
		}
		return true;
	}
	
	/** Compare Header and Footer Values with previous schema 
	 * @param Old Schema
	 * @param MetaInformation Map
	 * @return boolean(true/false)
	 */
	public static boolean compareHeaderandFooterValues(JSONObject oldSchema,Map<String,String> metaInformationMap){
		try{
			String header = metaInformationMap.get(ExcelConstants.HEADER_FORMAT);
			String footer = metaInformationMap.get(ExcelConstants.FOOTER_FORMAT);
			JSONObject metaInfo = oldSchema.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
			if(metaInfo.has(MetaInfoConstants.HEADER_ORIGIN)){
				if(metaInfo.has(MetaInfoConstants.FOOTER_ORIGIN)){
					if(header.equalsIgnoreCase(ParserUtils.cleanString(metaInfo.getString(MetaInfoConstants.HEADER_ORIGIN)))){
						if(footer.equalsIgnoreCase(ParserUtils.cleanString(metaInfo.getString(MetaInfoConstants.FOOTER_ORIGIN)))){
							return true;
						}else{
							return false;
						}
					}else{
						return false;
					}
				}else{
					return false;	
				}					
			}else{
				return false;
			}
		}catch(JSONException e){
			log.error(e.getMessage());
			return false;
		}
	}
	
	/** Update threshold value in existing JSON
	 * @param oldSourceSchema
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param tableName
	 * @param threshold
	 * @return boolean (true/false)
	 */
	public static boolean updateThresholdValueInJSON(JSONObject oldSourceSchema,Map<String,String> propertiesMap,String srcSystem,String country,String tableName,String threshold){
		
		JSONObject metaInfoSchema = new JSONObject();
		HDFSHandle hdfsHandler = new HDFSHandle();
		
		try {
			metaInfoSchema = oldSourceSchema.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
			metaInfoSchema.put(MetaInfoConstants.THRESHOLD_LIMIT, threshold);
			oldSourceSchema.put(JSONConstants.METAINFORMATION_SCHEMA, metaInfoSchema);
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
}
