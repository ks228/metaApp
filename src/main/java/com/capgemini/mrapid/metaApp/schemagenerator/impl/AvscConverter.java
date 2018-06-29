/**
 * AvscConverter class convert Destination json element to avro compatible schema
 * @author Pallavi Kadam
 *
 */
package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;



import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.utils.JsonUtils;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;

public class AvscConverter {
	
	final static  org.apache.log4j.Logger log = Logger.getLogger(AvscConverter.class);
	
	/**
	 * get default values of cols from destination schema in avsc file
	 * @param avscFields : avsc field
	 * @param avroTypeIs : the equivalent orc to avro datatype
	 * @param schemaField : The field from destination
	 */
	private Map<String, Object> getDefaultValue(Map<String, Object>avscFieldIs,String avroTypeIs, Map<String,Object> schemaField,String isNullable)
	{
		Map<String, Object> avscFieldWithDefault = avscFieldIs;
		if(schemaField.get(JSONConstants.COLUMN_DEFAULT).toString().isEmpty())
		{
			avscFieldWithDefault.put("default", schemaField.get(JSONConstants.COLUMN_DEFAULT));
			avscFieldWithDefault.put(JSONConstants.AVRO_TYPE, avroTypeIs);
			
		}
		else if(schemaField.get(JSONConstants.COLUMN_DEFAULT).toString().equalsIgnoreCase("null"))
		{
			avscFieldWithDefault.put("default", null);
			ArrayList<Object> type = new ArrayList<Object>();
			type.add("null");
			type.add(avroTypeIs);
			avscFieldWithDefault.put(JSONConstants.AVRO_TYPE, type);
		}
		else{
			
			try
			{
				if(avroTypeIs.equalsIgnoreCase("string"))
				{
					avscFieldWithDefault.put("default", (String) schemaField.get(JSONConstants.COLUMN_DEFAULT));
				}
				else if(avroTypeIs.equalsIgnoreCase("double"))
				{
					Double defaultVal = Double.parseDouble(schemaField.get("default")
							.toString());
					avscFieldWithDefault.put("default", defaultVal);
				}
				else if(avroTypeIs.equalsIgnoreCase("int"))
				{
					int defaultVal = Integer.parseInt(schemaField.get("default")
							.toString());
					avscFieldWithDefault.put("default", defaultVal);
				}
				else if(avroTypeIs.equalsIgnoreCase("bytes"))
				{
					Byte defaultVal = (Byte) schemaField.get("default");
					avscFieldWithDefault.put("default", defaultVal);
				}
				else
				{
					avscFieldWithDefault.put("default", schemaField.get(JSONConstants.COLUMN_DEFAULT));			
				}
			}
			catch(Exception e)
			{
				avscFieldWithDefault.put("default", schemaField.get(JSONConstants.COLUMN_DEFAULT));	
				log.info("Not able to type cast in AVSC CONVERTER");				
			}
			 if (isNullable.equalsIgnoreCase(JSONConstants.NOT_NULL_FALSE)) {
					ArrayList<Object> type = new ArrayList<Object>();
					type.add("null");
					type.add(avroTypeIs);
					avscFieldIs.put(JSONConstants.AVRO_TYPE, type);
				}
				else{
					avscFieldIs.put(JSONConstants.AVRO_TYPE, avroTypeIs);
				}
		}
		
			
		return avscFieldWithDefault;
	}
	
	/** Converts fields to avsc compatible fields
	 * @param avroDataType map from property file
	 * @param schemaField
	 * @param addTables
	 * @return Converted Avsc field
	 */
	private Map<String, Object> orcToAvroConversion(LinkedHashMap<String, Map<String, String>> avroDataTypeMap,
			Map<String,Object> schemaField, List<String> addTables)
	{
		Map<String, Object> avscFieldIs = new LinkedHashMap<String, Object>();
		String typeIs = schemaField.get(JSONConstants.COLUMN_TYPE).toString();
		String isNullable = schemaField.get(JSONConstants.COLUMN_NULLABLE).toString();
		
		String colName = schemaField.get(JSONConstants.COLUMN_NAME).toString();
		
		avscFieldIs.put(JSONConstants.AVRO_NAME, colName);
		
		Map<String, String> avroTypeInfo = new HashMap<String, String>();
		
		if(JsonUtils.isKeyExists(avroDataTypeMap,typeIs))
		{
			avroTypeInfo = avroDataTypeMap.get(typeIs);
		}
		
		String avroTypeIs = avroTypeInfo.get(JSONConstants.AVRO_TYPE);
		
		if(avroTypeInfo.containsKey(JSONConstants.AVRO_PROP_LOGIC_TYPE)){
			avscFieldIs.put(JSONConstants.AVRO_LOGIC_TYPE, avroTypeInfo.get(JSONConstants.AVRO_PROP_LOGIC_TYPE));
		}
			
		if (avroTypeInfo.containsKey(JSONConstants.AVRO_PRECISION)) {
			if (avroTypeInfo.get(JSONConstants.AVRO_PRECISION).equalsIgnoreCase("true")) {
				avscFieldIs.put(JSONConstants.AVRO_PRECISION, schemaField.get(JSONConstants.AVRO_PRECISION));
			}			
		}
		
		if (avroTypeInfo.containsKey(JSONConstants.AVRO_SCALE)) {
			if (avroTypeInfo.get(JSONConstants.AVRO_SCALE).equalsIgnoreCase("true")) {
				avscFieldIs.put(JSONConstants.AVRO_SCALE, schemaField.get(JSONConstants.AVRO_SCALE));
			}			
		}
		
		if (JsonUtils.isKeyExists(schemaField, JSONConstants.COLUMN_DEFAULT)) {
			avscFieldIs = getDefaultValue(avscFieldIs,avroTypeIs,schemaField,isNullable);
		}else{
			 if (isNullable.equalsIgnoreCase(JSONConstants.NOT_NULL_FALSE)) {
					ArrayList<Object> type = new ArrayList<Object>();
					type.add("null");
					type.add(avroTypeIs);
					avscFieldIs.put(JSONConstants.AVRO_TYPE, type);
				}
				else{
					avscFieldIs.put(JSONConstants.AVRO_TYPE, avroTypeIs);
				}
		}
				
		return avscFieldIs;
	}
	
	
//	/** Get columns to skip 
//	 * @param propertiesMap
//	 * @return list of columns
//	 */
//	private ArrayList<String> getColumnsToSkip(Map<String, String> propertiesMap) {
//		 ArrayList<String> skipCols = new ArrayList<String>();
//		 if(propertiesMap.containsKey(Constants.BUSS_JOURNAL_DATE_TIME_COLS)){
//			String  bussJournal_col=propertiesMap.get(Constants.BUSS_JOURNAL_DATE_TIME_COLS);
//		 	String[] bussJournalDateTimeCol = bussJournal_col.split(",");
//		 	if(bussJournalDateTimeCol.length > 0){
//		 		for(int i=0; i < bussJournalDateTimeCol.length;i++){
//			 		skipCols.add(ParserUtils.cleanString(bussJournalDateTimeCol[i]));
//		 		}
//		 	}
//		 }
//		return skipCols;
//			
//	}
	

	/**
	 * Read Destination Schema and convert each field to Avro Schema
	 * NOTE : name in avro schema is table name from MetaInformation of jsonStr 
	 * @param json - Generated by Parser
	 * @param propertiesMap
	 * @param srcSystem 
	 * @param country
	 * @param table
	 * @param datatypeMap : comes from property file
	 * @param addTables
	 */
	public String convert(JSONObject json,Map<String, String> propertiesMap, String srcSystem, String country,
			String table, Map<String,String> datatypeMap, List<String> addTables,boolean is_dynamic_partition) {
		
			LinkedHashMap<String, Map<String, String>> avroDataTypeMap = ParserUtils.getAvroTypeConversion(datatypeMap);
			LinkedHashMap<String, Object> jsonObj = new LinkedHashMap<String, Object>();
			LinkedHashMap<String, Object> avscSchema = new LinkedHashMap<String, Object>();
			
			jsonObj = JsonUtils.parseJSON(json);
			@SuppressWarnings("unchecked")
			LinkedHashMap<String, Object> destinationSchema = (LinkedHashMap<String, Object>) jsonObj
					.get(JSONConstants.DESTINATION_SCHEMA);
			
			// From Destination schema take columns array
			@SuppressWarnings("unchecked")
			ArrayList<LinkedHashMap<String, Object>> columns = (ArrayList<LinkedHashMap<String, Object>>) destinationSchema
					.get(JSONConstants.COLUMNS_ARRAY);
		
			ArrayList<Object> avscFields = new ArrayList<Object>();
			
//			ArrayList<String> skipColumns = new ArrayList<String>();
//			skipColumns = getColumnsToSkip(propertiesMap);
//			String partition_col = propertiesMap.get(Constants.TABLE_PARTITION);
			String partition_col = "";
			// Iterate on each field to convert it into avro schema
			for (int i = 0; i < columns.size(); i++) {
				Map<String, Object> schemaField = columns.get(i);
				Map<String, Object> avscFieldIs = new LinkedHashMap<String, Object>();
				String colName = schemaField.get(JSONConstants.COLUMN_NAME).toString();
				if(is_dynamic_partition)
					if(partition_col.equalsIgnoreCase(colName))
						continue;
//				int indexCol = skipColumns.indexOf(colName);
//				if(indexCol == -1)
//				{
					avscFieldIs.put(JSONConstants.AVRO_NAME, schemaField.get(JSONConstants.COLUMN_NAME));
					avscFieldIs = orcToAvroConversion(avroDataTypeMap,schemaField,addTables);
					avscFields.add(avscFieldIs);
//				}				
			}
			avscSchema.put(JSONConstants.AVRO_NAME, table);
			avscSchema.put(JSONConstants.AVRO_TYPE, JSONConstants.AVRO_RECORD);
			avscSchema.put(JSONConstants.AVRO_FIELDS, avscFields);

			String avscSchemaJson = JsonUtils.MaptoJSON(avscSchema);
			return avscSchemaJson;
			
	}
}