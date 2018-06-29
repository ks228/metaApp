/**
 *  HiveAvroConverter Create Hive HQL to store Avro Data 
 * @author Neha Bagadia
 */

package com.capgemini.mrapid.metaApp.schemagenerator.impl;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.log4j.Logger;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;


public class HiveAvroConverter {
	final static Logger log = Logger.getLogger(HiveAvroConverter.class);

/**
 * Generate hive create table query in orc format
 * @param propertiesMap
 * @param srcSystem
 * @param country
 * @param table
 * @param classification
 * @param schemaVersion
 * @return
 */
public static String convert(Map<String, String> propertiesMap ,String srcSystem,String country,String table,Integer schemaVersion,boolean is_staging,JSONObject schemaInformationObject) {
	
		
		StringBuilder query = new StringBuilder();
		log.info(srcSystem+":"+country+":"+table+":"+"Creating target Hive ddl using avro schema");
		try{
			JSONObject jsonMetaInformationJson = (JSONObject) schemaInformationObject.get(JSONConstants.METAINFORMATION_SCHEMA);
			JSONObject jsonDestinationschemaObject =   (JSONObject) schemaInformationObject.get(JSONConstants.DESTINATION_SCHEMA); 
			JSONArray partitionColArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.PARTITION_COLS);

			
			String tabletypeis = ParserUtils.cleanString(jsonMetaInformationJson.getString(JSONConstants.TARGET_TABLE_TYPE)).toUpperCase();
			String compressionType = ParserUtils.cleanString(jsonMetaInformationJson.getString(JSONConstants.TARGET_TABLE_COMPRESSION)).toUpperCase();
			String table_location;
			System.out.println("TABLE TYPE IS:" + tabletypeis);
			
			if(is_staging){
				table_location = propertiesMap.get(Constants.HIVE_BASE_PATH_STAGING);
				query.append("CREATE TABLE IF NOT EXISTS " + propertiesMap.get(Constants.HIVE_STAGING_DB)+"."+ srcSystem + "_" + country + "_" + table +"\n");
			}			
			else{
				table_location = propertiesMap.get(Constants.HIVE_BASE_PATH_PROCESSING);
				if(tabletypeis.equalsIgnoreCase("INTERNAL"))
				{
					query.append("CREATE TABLE IF NOT EXISTS " + propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table +"\n");
				}
				else{
					query.append("CREATE "+ tabletypeis + " TABLE IF NOT EXISTS " + propertiesMap.get(Constants.HIVE_PROCESSING_DB)+"."+ srcSystem + "_" + country + "_" + table +"\n");
				}
			}
	
			if(!tabletypeis.equalsIgnoreCase("TEMPORARY")){
				if(partitionColArray.length() > 0){
					query.append("PARTITIONED BY ("+ParserUtils.createTexttableSchemaQuery(partitionColArray,"") + ")\n");
				}
			}
			
			if(is_staging || compressionType.isEmpty() || compressionType.equalsIgnoreCase("null") || compressionType.equalsIgnoreCase("none") || compressionType.equalsIgnoreCase("default")){
				query.append("ROW FORMAT\nSERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' WITH SERDEPROPERTIES(\"serialization.encoding\"='" + propertiesMap.get(Constants.ENCODINGCHARSET) + "')\n");
			}
			else
			{
				query.append("ROW FORMAT\nSERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' WITH SERDEPROPERTIES(\"serialization.encoding\"='" + propertiesMap.get(Constants.ENCODINGCHARSET) + "',\"avro.compression\"='"+ compressionType + "')\n");
			}
			query.append("STORED AS AVRO \n");
			
			
			
			String avroOutputDir= propertiesMap.get(Constants.AVSC_OUTPUT_DIR)+"/" + srcSystem + "_" + country + "_" + table + Constants.AVSC_FORMAT;
			
			
			
			query.append("LOCATION '" + table_location+"'\n");
			query.append("TBLPROPERTIES('avro.schema.url'='"+ avroOutputDir+"');\n");
		}catch(JSONException e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
	
		return query.toString();
	}

}
