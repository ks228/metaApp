/**
 * AvscConverter class convert Destination json element to avro compatible schema
 * @author Pallavi Kadam
 *
 */
package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;




import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.pojo.HiveMetaStructure;
import com.capgemini.mrapid.metaApp.utils.JsonUtils;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.google.gson.Gson;

public class TextConverter {
	
final static Logger log = Logger.getLogger(TextConverter.class);
	
	/**
	 * Generate hive create table query in text format
	 * @param jsonObject
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @return
	 */
	public static String convert(JSONObject jsonObject,Map<String, String> propertiesMap ,String srcSystem,String country,String table,boolean is_temp){
		
		Gson gson = new Gson();
		StringBuilder query = new StringBuilder();
		
		try{
		JSONObject jsonMetaInformationJson = (JSONObject) jsonObject.get(JSONConstants.METAINFORMATION_SCHEMA);
		HiveMetaStructure jsonMetaInformationObject = gson.fromJson(jsonMetaInformationJson.toString(), HiveMetaStructure.class);
		
		JSONObject jsonDestinationschemaObject = (JSONObject) jsonObject.get(JSONConstants.DESTINATION_SCHEMA); 
		
		JSONObject jsonSourceschemaObject = (JSONObject) jsonObject.get(JSONConstants.SOURCE_SCHEMA); 
		JSONArray jsonSourceschemaArray = (JSONArray) jsonSourceschemaObject.get(JSONConstants.COLUMNS_ARRAY);
		
		JSONArray jsonDestinationschemaArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.COLUMNS_ARRAY);
		
		JSONArray partitionColArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.PARTITION_COLS);
		
//		String partition_col = propertiesMap.get(Constants.TABLE_PARTITION);
		String partition_col = "";
		String tablelocation;
		String tabletypeis = ParserUtils.cleanString(jsonMetaInformationJson.getString(JSONConstants.TARGET_TABLE_TYPE)).toUpperCase();
		log.info(srcSystem+":"+country+":"+table+":"+"Creating target Hive ddl");
		query.append("no|");
		if(is_temp){
			JSONArray final_array = ParserUtils.createTempTexttableSchemaQuery(jsonSourceschemaArray,jsonDestinationschemaArray, partitionColArray);
			query.append("CREATE TABLE IF NOT EXISTS " +propertiesMap.get(Constants.HIVE_TEMP_DB)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
			tablelocation = propertiesMap.get(Constants.HIVE_BASE_PATH_TEMP);
			query.append(ParserUtils.createTexttableSchemaQuery(final_array,partition_col));
		}
		else
		{
			if(!tabletypeis.equalsIgnoreCase("internal"))
			{
				query.append("CREATE " + tabletypeis + " TABLE IF NOT EXISTS " +propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
			}
			else
			{
				query.append("CREATE TABLE IF NOT EXISTS " +propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
			}
			query.append(ParserUtils.createTexttableSchemaQuery(jsonDestinationschemaArray,partition_col));
			tablelocation = propertiesMap.get(Constants.HIVE_BASE_PATH_PROCESSING);
		}
		
		
		query.append(") \n");
		if(!(tabletypeis.equalsIgnoreCase("TEMPORARY")||is_temp)){
			if(partitionColArray.length() > 0){
				query.append("PARTITIONED BY ("+ParserUtils.createTexttableSchemaQuery(partitionColArray,"") + ")\n");
			}
			
		}
		query.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + jsonMetaInformationObject.getTgtdelim() + "' ");		
		query.append("STORED AS TEXTFILE\n");
		query.append("LOCATION '" +  tablelocation  + "'; \n");
		}catch(JSONException e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
		return query.toString();
	}
}