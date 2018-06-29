package com.capgemini.mrapid.metaApp.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.ExcelConstants;
import com.capgemini.mrapid.metaApp.constants.MetaInfoConstants;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.Generator;
import com.capgemini.mrapid.metaApp.utils.JsonUtils;


/**
 * Class MetaInfoGenerator : generates  target schema MetaInformation 
 * @author Anuradha Dede
 */

public class MetaInfoGenerator {

	final static Logger log = Logger.getLogger(Generator.class);
	
	/**
	 * getMetaInfoJsonObject create target Meta information  
	 * @param countryPropertiesMap
	 * @param sourcePropertiesMap
	 * @param propertiesMap
	 * @param table
	 * @param country
	 * @return returns JSONObject has target Metainformation
	 */
	 
	public static JSONObject getMetaInfoJsonObject(Map<String, String> countryPropertiesMap,Map<String, String> sourcePropertiesMap,Map<String,String> propertiesMap,TableListStructure table,String country) {
		Map<String, String> convertedSchema = new HashMap<String, String>();
		JSONObject metaInfoObject = null;
		try{
		convertedSchema.put(MetaInfoConstants.SRCNAME, sourcePropertiesMap.get(Constants.SOURCE_NAME));
		convertedSchema.put(MetaInfoConstants.SRCTYPE, sourcePropertiesMap.get(Constants.SOURCE_TYPE));
		convertedSchema.put(MetaInfoConstants.SRCDBTYPE, sourcePropertiesMap.get(Constants.SOURCE_DB_TYPE));
		convertedSchema.put(MetaInfoConstants.SRCDELIM, sourcePropertiesMap.get(Constants.SOURCE_DELIMETED));
		convertedSchema.put(MetaInfoConstants.SRCFREQ, sourcePropertiesMap.get(Constants.SOURCE_FREQUENCY));
		convertedSchema.put(MetaInfoConstants.SRCDATEFORMAT, countryPropertiesMap.get(Constants.SOURCE_DATE_FORMAT));
		convertedSchema.put(MetaInfoConstants.SRCTIMEFORMAT, countryPropertiesMap.get(Constants.SOURCE_TIME_FORMAT));
		if(sourcePropertiesMap.get(Constants.SOURCE_TABLE_SPACE) != null)
			convertedSchema.put(MetaInfoConstants.SRCTABLESPACE, sourcePropertiesMap.get(Constants.SOURCE_TABLE_SPACE));
		else 
			convertedSchema.put(MetaInfoConstants.SRCTABLESPACE, countryPropertiesMap.get(Constants.SOURCE_TABLE_SPACE));
		convertedSchema.put(MetaInfoConstants.TGTDBTYPE, propertiesMap.get(Constants.TARGET_DB_TYPE));
		convertedSchema.put(MetaInfoConstants.TGTDB, sourcePropertiesMap.get(Constants.TARGET_DB));
		convertedSchema.put(MetaInfoConstants.TGTDELIM, sourcePropertiesMap.get(Constants.TARGET_DELIMETED));
		convertedSchema.put(MetaInfoConstants.TGTDBFORMAT, sourcePropertiesMap.get(Constants.TARGET_DB_FORMAT));
		convertedSchema.put(MetaInfoConstants.TGTPROCESSEDFORMAT, sourcePropertiesMap.get(Constants.TARGET_PROCESSED_FORMAT));
		convertedSchema.put(MetaInfoConstants.CLASSIFICATION,table.classification);
		convertedSchema.put(MetaInfoConstants.TABLENAME, table.name);
		convertedSchema.put(MetaInfoConstants.THRESHOLD_LIMIT, table.threshold);
		convertedSchema.put(MetaInfoConstants.COUNTRY, country);
		convertedSchema.put(MetaInfoConstants.CHARACTERSET,sourcePropertiesMap.get(Constants.ENCODINGCHARSET));
		
		metaInfoObject = new JSONObject(JsonUtils.MaptoJSON(convertedSchema));
		
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		
		return metaInfoObject;
	}
	
	/**
	 * getMetaInfoJsonObject creates meta information object for excel
	 * @param excelPropertyMap
	 * @param sourcePropertiesMap
	 * @param table
	 * @param country
	 * @return
	 */
	public static JSONObject getMetaInfoJsonObject(Map<String, String> excelPropertyMap,Map<String, String> sourcePropertiesMap,TableListStructure table,String country) {
		Map<String, String> convertedSchema = new HashMap<String, String>();
		JSONObject metaInfoObject = null;
		try{
		convertedSchema.put(MetaInfoConstants.SRCNAME, excelPropertyMap.get(ExcelConstants.SOURCE_NAME));
		convertedSchema.put(MetaInfoConstants.SRCTYPE, excelPropertyMap.get(ExcelConstants.SOURCE_TYPE));
		convertedSchema.put(MetaInfoConstants.SRCDBTYPE, excelPropertyMap.get(ExcelConstants.SOURCE_DB_TYPE));
		convertedSchema.put(MetaInfoConstants.SRCDELIM, excelPropertyMap.get(ExcelConstants.SOURCE_DELIMETED));
//		convertedSchema.put(MetaInfoConstants.SRCFREQ, excelPropertyMap.get(ExcelConstants.SOURCE_FREQUENCY));
		convertedSchema.put(MetaInfoConstants.SRCDATEFORMAT, excelPropertyMap.get(ExcelConstants.SOURCE_DATE_FORMAT));
		convertedSchema.put(MetaInfoConstants.SRCTIMEFORMAT, excelPropertyMap.get(ExcelConstants.SOURCE_TIME_FORMAT));
		convertedSchema.put(MetaInfoConstants.TGTDBTYPE, sourcePropertiesMap.get(Constants.TARGET_DB_TYPE));
		convertedSchema.put(MetaInfoConstants.TGTDB, sourcePropertiesMap.get(Constants.TARGET_DB));
		convertedSchema.put(MetaInfoConstants.TGTDELIM, sourcePropertiesMap.get(Constants.TARGET_DELIMETED));
		convertedSchema.put(MetaInfoConstants.TGTDBFORMAT, excelPropertyMap.get(ExcelConstants.EXCEL_TARGET_DB));
		convertedSchema.put(MetaInfoConstants.TGTPROCESSEDFORMAT, excelPropertyMap.get(ExcelConstants.EXCEL_TARGET_DB));
		convertedSchema.put(MetaInfoConstants.CLASSIFICATION,table.classification);
		convertedSchema.put(MetaInfoConstants.TABLENAME, table.name);
		convertedSchema.put(MetaInfoConstants.THRESHOLD_LIMIT, table.threshold);
		convertedSchema.put(MetaInfoConstants.COUNTRY, country);
//		convertedSchema.put(MetaInfoConstants.CHARACTERSET, excelPropertyMap.get(ExcelConstants.CHARACTERSET));
		convertedSchema.put(MetaInfoConstants.IS_HEADER_FOOTER, sourcePropertiesMap.get(Constants.IS_HEADER_FOOTER));
//		convertedSchema.put(MetaInfoConstants.HEADER_DATE_FORMAT, excelPropertyMap.get(ExcelConstants.HEADER_DATE_FORMAT));
//		convertedSchema.put(MetaInfoConstants.HEADER_LENGTH, excelPropertyMap.get(ExcelConstants.HEADER_LENGTH));
//		convertedSchema.put(MetaInfoConstants.FOOTER_LENGTH, excelPropertyMap.get(ExcelConstants.FOOTER_LENGTH));
//		convertedSchema.put(MetaInfoConstants.DATA_LENGTH, excelPropertyMap.get(ExcelConstants.DATA_LENGTH));
		convertedSchema.put(MetaInfoConstants.CHECKSUM_COLUMN, excelPropertyMap.get(ExcelConstants.CHECKSUM_COLUMN));
		convertedSchema.put(MetaInfoConstants.CHECKSUM_POSITION, excelPropertyMap.get(ExcelConstants.CHECKSUM_POSITION));
//		convertedSchema.put(MetaInfoConstants.FILE_TYPE, excelPropertyMap.get(ExcelConstants.FILE_TYPE));
		
		convertedSchema.put("targettabletype",excelPropertyMap.get(ExcelConstants.TRAGET_TABLE_TYPE));
		convertedSchema.put("datacompression",excelPropertyMap.get(ExcelConstants.DATA_COMPRESSION));
//		convertedSchema.put("partitioncol",excelPropertyMap.get(ExcelConstants.EXCEL_PARTITION_COL));
		metaInfoObject = new JSONObject(JsonUtils.MaptoJSON(convertedSchema));
		
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		
		return metaInfoObject;
	}
	
	
	
}
