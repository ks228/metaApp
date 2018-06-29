/**
 *  Class - HiveORCSchemaEvolution Creates Hive HQL to store ORC Data in it. Also stores Hive HQL on HDFS 
 *  @author Anurag Udasi
 */

package com.capgemini.mrapid.metaApp.schemagenerator.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.RemedyConstants;
import com.capgemini.mrapid.metaApp.pojo.HiveColumnStructure;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.capgemini.mrapid.metaApp.utils.RemedyLogsUtils;
import com.capgemini.mrapid.metaApp.utils.SchemaEvolution;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;



public class HiveORCSchemaEvolution {
	final static Logger log = Logger.getLogger(HiveORCSchemaEvolution.class);
	
	/**
	 * Generate hive ORC table alter/create query based on schema evolution
	 * @param newSourceSchemaObject
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param oldSourceSchemaObject
	 * @param dataTypeMappingPropertiesMap
	 * @param deploymentProperties
	 * @return
	 */
	public static String convert(JSONObject newSourceSchemaObject,Map<String, String> propertiesMap ,String srcSystem,String country,String table,JSONObject oldSourceSchemaObject,Map<String,String> dataTypeMappingPropertiesMap,Map<String,String> deploymentProperties){
		
		StringBuilder query = new StringBuilder();
		boolean isRetroFit = false;
		
		log.info(srcSystem+":"+country+":"+table+":"+"Creating target Hive ddl");
		
		try{
			JSONObject oldSourceSchema = oldSourceSchemaObject.getJSONObject(JSONConstants.SOURCE_SCHEMA);
			JSONObject newSourceSchema = newSourceSchemaObject.getJSONObject(JSONConstants.SOURCE_SCHEMA);
			JSONObject newDescSchema = newSourceSchemaObject.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
			
			JSONArray oldSourceArray = oldSourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
			JSONArray newSourceArray = newSourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY); 
			JSONArray newDescArray = newDescSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY);
		
			
			Map<String,HiveColumnStructure> oldSchemaMap = HiveORCSchemaEvolution.getSchemaMap(oldSourceArray);
			Map<String,HiveColumnStructure> newSchemaMap = HiveORCSchemaEvolution.getSchemaMap(newSourceArray);
			TreeMap<Integer,HiveColumnStructure> finalSchemaMap = new TreeMap<Integer,HiveColumnStructure>();
			int mapDifference = newSchemaMap.size() - oldSchemaMap.size();
			
			if(newSourceArray.length() > oldSourceArray.length()){
		
				for (Map.Entry<String, HiveColumnStructure> newEntry : newSchemaMap.entrySet())
				{
					boolean flag = true;
					boolean isAddOlny = true;
					
					for(Map.Entry<String, HiveColumnStructure> oldEntry : oldSchemaMap.entrySet()){
						if(newEntry.getKey().toString().equalsIgnoreCase(oldEntry.getKey().toString())){
							String seqOld = oldEntry.getValue().getColseq();
							String seqNew = newEntry.getValue().getColseq();
							String dataTypeOld = oldEntry.getValue().getColtype();
							String dataTypeNew = newEntry.getValue().getColtype();
							if(!dataTypeOld.equalsIgnoreCase(dataTypeNew))
								isAddOlny = false;
							else if(!seqOld.equalsIgnoreCase(seqNew))
								isAddOlny = false;
							else
								flag = false;
							break;
						}   
					}
					if(!isAddOlny){
						isRetroFit = true;
						break;
					}	
					if(flag)
						finalSchemaMap.put(Integer.parseInt(newEntry.getValue().getColseq()), newEntry.getValue());
				}
			}else{
				isRetroFit = true;
			}
			
			if(finalSchemaMap.size() > 0 && mapDifference == finalSchemaMap.size() && !isRetroFit){
			query.append("no|");
				log.info("Creating alter statement for ORC schema evolution");
				query.append("ALTER TABLE " + propertiesMap.get(Constants.HIVE_PROCESSING_DB) + "." + srcSystem + "_" + country + "_" + table +" SET FILEFORMAT ORC;" + "\n");
				for (Map.Entry<Integer, HiveColumnStructure> entry : finalSchemaMap.entrySet())
				{
					String datatype = ParserUtils.convertDataTypeForHive(entry.getValue().getColtype(), dataTypeMappingPropertiesMap, Integer.parseInt(entry.getValue().getColprecision()), Integer.parseInt(entry.getValue().getColscale()));
					query.append("ALTER TABLE " + propertiesMap.get(Constants.HIVE_PROCESSING_DB) + "." + srcSystem + "_" + country + "_" + table + " ADD COLUMNS (" + entry.getValue().getColname() + " " + datatype);
					if(datatype.equalsIgnoreCase("decimal")){
						query.append(" (" +  entry.getValue().getColprecision() + "," + entry.getValue().getColscale() + "));\n");
					}else{
						query.append(");\n"); 
					}
				}
			}else{
				log.info("Schema change with retrofit scenario has been encounterd");
				isRetroFit = true;
			}
		
		if(isRetroFit){
			query.append("yes|");
			query.append(SchemaEvolution.createORCRetroFitQuery(propertiesMap, newDescArray,srcSystem,country,table));
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYONE,
					RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYONE),null,deploymentProperties);
		}
		
		}catch(JSONException e){
			log.error(e.getMessage());
		}
		return query.toString();
	}
	
	/**
	 * @param schemaArray
	 * @throws JsonParseException
	 * @throws JSONException
	 * @return
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
}
