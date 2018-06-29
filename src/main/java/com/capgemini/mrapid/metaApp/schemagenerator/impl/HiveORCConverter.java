/**
 *  HiveORCConverter Create Hive HQL to store ORC Data 
 *  @author Anurag Udasi
 */

package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.pojo.HiveMetaStructure;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.google.gson.Gson;

public class HiveORCConverter {
	final static Logger log = Logger.getLogger(HiveORCConverter.class);
	
	/**
	 * Generate hive create table query in orc format
	 * @param jsonObject
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @return
	 */
	public static String convert(JSONObject jsonObject,Map<String, String> propertiesMap ,String srcSystem,String country,String table){
		
		Gson gson = new Gson();
		StringBuilder query = new StringBuilder();
		
		try{
		JSONObject jsonMetaInformationJson = (JSONObject) jsonObject.get(JSONConstants.METAINFORMATION_SCHEMA);
		HiveMetaStructure jsonMetaInformationObject = gson.fromJson(jsonMetaInformationJson.toString(), HiveMetaStructure.class);
		
		String tabletypeis = ParserUtils.cleanString(jsonMetaInformationJson.getString(JSONConstants.TARGET_TABLE_TYPE)).toUpperCase();
		String compressionType = ParserUtils.cleanString(jsonMetaInformationJson.getString(JSONConstants.TARGET_TABLE_COMPRESSION)).toUpperCase();
		
		JSONObject jsonDestinationschemaObject =   (JSONObject) jsonObject.get(JSONConstants.DESTINATION_SCHEMA); 
		JSONArray jsonDestinationschemaArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.COLUMNS_ARRAY);
		JSONArray partitionColArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.PARTITION_COLS);

		
//		String partition_col = propertiesMap.get(Constants.TABLE_PARTITION);
		
		System.out.println("TABLE TYPE IS:" + tabletypeis);
		
		
		log.info(srcSystem+":"+country+":"+table+":"+"Creating target Hive ddl");
		query.append("no|");
		if(tabletypeis.equalsIgnoreCase("INTERNAL"))
		{
			query.append("CREATE TABLE IF NOT EXISTS " +propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
		}
		else{
			query.append("CREATE "+ tabletypeis + " TABLE IF NOT EXISTS " +propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
		}
		query.append(ParserUtils.createORCtableSchemaQuery(jsonDestinationschemaArray,""));
		query.append(") \n");
		if(!tabletypeis.equalsIgnoreCase("TEMPORARY")){
			if(partitionColArray.length() > 0){
				query.append("PARTITIONED BY ("+ParserUtils.createTexttableSchemaQuery(partitionColArray,"") + ")\n");
			}
		}
		if(compressionType.isEmpty() || compressionType.equalsIgnoreCase("null") || compressionType.equalsIgnoreCase("none") || compressionType.equalsIgnoreCase("default")){
			query.append("ROW FORMAT \nSERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES(\"serialization.encoding\"='" + propertiesMap.get(Constants.ENCODINGCHARSET)  + "',\"field.delim\"='" + jsonMetaInformationObject.getTgtdelim() +"',\"serialization.format\"='"+ jsonMetaInformationObject.getTgtdelim() + "')\n");
		}
		else{
			query.append("ROW FORMAT \nSERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES(\"serialization.encoding\"='" + propertiesMap.get(Constants.ENCODINGCHARSET)  + "',\"field.delim\"='" + jsonMetaInformationObject.getTgtdelim() +"',\"serialization.format\"='"+ jsonMetaInformationObject.getTgtdelim() +"',\"orc.compression\"='"+ compressionType + "')\n");
		}
		
		
		query.append("STORED AS " + jsonMetaInformationObject.getTgtdbformat()  +"\n");
		query.append("LOCATION '" +  propertiesMap.get(Constants.HIVE_BASE_PATH_PROCESSING)  + "'; \n");
		}catch(JSONException e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
		return query.toString();
	}
}
