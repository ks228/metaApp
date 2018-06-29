package com.capgemini.mrapid.metaApp.utils;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.pojo.HiveColumnStructure;
import com.capgemini.mrapid.metaApp.pojo.HiveMetaStructure;
import com.google.gson.Gson;

/**
 * Class to create hive recon table DDL query
 * @author Anurag Udasi
 *
 */
public class ReconTableCreation {
	final static Logger log = Logger.getLogger(ReconTableCreation.class);

	/**
	 * Create JSON file for Recon tables
	 * @param fileName
	 * @param schemaInformationObject
	 * @param propertiesMap
	 * @return
	 */
	public boolean createReconJsonFile(String fileName,String schemaInformationObject,Map<String,String> propertiesMap){
		boolean result = false;
		try{
			HDFSHandle hdfsHandler = new HDFSHandle();
			result = hdfsHandler.hdfsWrite(schemaInformationObject, propertiesMap,fileName, propertiesMap.get(Constants.JSON_OUTPUT_DIR));
			if (result) {
				log.info("file created at " + propertiesMap.get(Constants.JSON_OUTPUT_DIR) + "/" + fileName);
			}
			}catch(Exception e){
				log.error(e.getMessage());
			}
		return result;
	}
	
	
	/**
	 * Create HQL file for Recon tables
	 * @param orcFilename
	 * @param schemaInformationObject
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param deploymentMap
	 * @return
	 */
	public boolean createReconHqlFile(String orcFilename,JSONObject schemaInformationObject,Map<String,String> propertiesMap,String srcSystem,String country,String table,Map<String,String> deploymentMap){
		boolean result = false;
		try{
		HDFSHandle hdfsHandler = new HDFSHandle();
		String reconHQL = createReconHql(schemaInformationObject,propertiesMap,srcSystem,country,table,deploymentMap);
		result = hdfsHandler.hdfsWrite(reconHQL, propertiesMap,orcFilename, propertiesMap.get(Constants.DDL_OUTPUT_DIR));
		if (result) {
			log.info("file created at " + propertiesMap.get(Constants.DDL_OUTPUT_DIR) + "/" + orcFilename);
		}
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return result;
	}
	
	
	/**
	 * Create ORC schema for Recon tables
	 * @param jsonObject
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param deploymentMap
	 * @return
	 */
	protected String createReconHql(JSONObject jsonObject,Map<String, String> propertiesMap ,String srcSystem,String country,String table,Map<String,String> deploymentMap){
		
		Gson gson = new Gson();
		StringBuilder query = new StringBuilder();
		
		try{
		JSONObject jsonDestinationschemaObject =   (JSONObject) jsonObject.get(JSONConstants.DESTINATION_SCHEMA); 
		JSONArray jsonDestinationschemaArray = (JSONArray) jsonDestinationschemaObject.get(JSONConstants.COLUMNS_ARRAY);
		JSONObject jsonMetaInformationJson = (JSONObject) jsonObject.get(JSONConstants.METAINFORMATION_SCHEMA);
		HiveMetaStructure jsonMetaInformationObject = gson.fromJson(jsonMetaInformationJson.toString(), HiveMetaStructure.class);
		
		log.info(srcSystem+":"+country+":"+table+":"+"Creating target Hive ddl");
		
		query.append("CREATE EXTERNAL TABLE IF NOT EXISTS " +deploymentMap.get(Constants.METADATA_DBNAME)+"."+ srcSystem + "_" + country + "_" + table + " ( \n");
        
		for (int i = 0; i < jsonDestinationschemaArray.length(); i++) {	
			HiveColumnStructure hiveColumnStructure = new HiveColumnStructure();
			hiveColumnStructure = gson.fromJson(jsonDestinationschemaArray.get(i).toString(), HiveColumnStructure.class); 
			if(hiveColumnStructure.getColname().equalsIgnoreCase("rowid"))
				continue;
			query.append("`" + hiveColumnStructure.getColname() + "`" + " " + hiveColumnStructure.getColtype());
			if(hiveColumnStructure.getColtype().contains("decimal")){
				query.append("(" + hiveColumnStructure.getColprecision() + "," + hiveColumnStructure.getColscale() + ")" );
			}
			if(hiveColumnStructure.getColtype().contains("varchar"))
				query.append("(" + hiveColumnStructure.getCollen()+ ")");
			if((i+1) != jsonDestinationschemaArray.length())
				query.append(",\n");
		}
			
		query.append(") \n");
		query.append("PARTITIONED BY ("+propertiesMap.get(Constants.TABLE_PARTITION) + " " + Constants.PARTITION_TYPE + ")\n");
		query.append("ROW FORMAT DELIMITED\n");
		query.append("FIELDS TERMINATED BY '" + jsonMetaInformationObject.getTgtdelim() + "'\n");
		query.append("LOCATION '" +  deploymentMap.get(Constants.COMMON_HIVE_LOCATION) + "/" + srcSystem + "_" + country + "_" + table +"'; \n");
		}catch(JSONException e){
			log.error(e.getMessage());
			e.printStackTrace();
		}
		return query.toString();
		
	}
	
}
