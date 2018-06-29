package com.capgemini.mrapid.metaApp.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.pojo.HiveColumnStructure;
import com.capgemini.mrapid.metaApp.utils.JsonUtils;
import com.google.gson.Gson;

/**
 * Class containg parser util methods
 * @author Anurag Udasi
 *
 */
public class ParserUtils {
	
	final static Logger log = Logger.getLogger(ParserUtils.class);
	
	/**
	 * convertDataTypelengthForHive:length of DataType
	 * 
	 * @param dataType
	 *            : datatype specified in DB2 DDL
	 * @param length
	 *            :length as per datatype specified in DB2 DDL
	 * @param datatypeMap
	 * 			  : Contains Data Type Mapping for DB Type coming from properties file
	 * @return String : dest schema datatype length
	 */
	public static Integer convertLengthForHive(String dataType, Integer length,Map<String,String> datatypeMap) {
		Integer destlength = 0;

		for (Map.Entry<String, String> entry : datatypeMap.entrySet())
		{
			String[] sourceLength= entry.getKey().split(JSONConstants.SEPERATOR);
			String destinationLength= entry.getValue();
			if(sourceLength.length > 1)
			{
				if(sourceLength[1].equalsIgnoreCase(JSONConstants.ORC_LENGTH_PREFIX)){
					if(sourceLength[0].equalsIgnoreCase(dataType)){
						if(destinationLength.contains("*")){
							String[] multiplyFactor =  destinationLength.split("\\*");
							destlength = Integer.parseInt(multiplyFactor[1]) * length;
							break;
						}else {
							destlength = Integer.parseInt(destinationLength);
							break;
						}
					}
				}
			}	
		}
		if(destlength == 0){
			destlength = length;
		}
		return destlength;
	}

	/**
	 * convertDataTypeForHive of DataType
	 * 
	 * @param dataType
	 *            : datatype specified in DB2 DDL
	 * @param datatypeMap
	 * 			  : Contains Data Type Mapping for DB Type coming from properties file
	 * @param precision
	 * @param scale
	 * @return String : destination schema datatype
	 */
    public static String convertDataTypeForHive(String dataType,
            Map<String, String> datatypeMap, Integer precision, Integer scale) {
     String type = new String();
     String default_type = new String();
     for (Map.Entry<String, String> entry : datatypeMap.entrySet()) {
            String[] sourceType = entry.getKey().split(JSONConstants.SEPERATOR);
            String destinationType = entry.getValue(); 
            if(sourceType.length == 2 && sourceType[0].equalsIgnoreCase(dataType) && sourceType[1].equalsIgnoreCase(JSONConstants.ORC_DATATYPE_PREFIX))
            {
            	type = destinationType;
    			break;
            }
            else if(sourceType.length == 3 && sourceType[0].equalsIgnoreCase(dataType) && sourceType[1].equalsIgnoreCase(JSONConstants.ORC_DATATYPE_PREFIX))
            {
            	String[] logicalCond = sourceType[2].split("\\*");
            	if(logicalCond[1].equalsIgnoreCase("gt"))
            	{
            		if(logicalCond[0].equalsIgnoreCase("scale") && scale > Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            		if(logicalCond[0].equalsIgnoreCase("precision") && precision > Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            			
            	}
            	else if(logicalCond[1].equalsIgnoreCase("gte"))
            	{
            		if(logicalCond[0].equalsIgnoreCase("scale") && scale >= Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            		if(logicalCond[0].equalsIgnoreCase("precision") && precision >= Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            			
            	}
            	else if(logicalCond[1].equalsIgnoreCase("lt"))
            	{
            		if(logicalCond[0].equalsIgnoreCase("scale") && scale < Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            		if(logicalCond[0].equalsIgnoreCase("precision") && precision < Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            			
            	}
            	else if(logicalCond[1].equalsIgnoreCase("lte"))
            	{
            		if(logicalCond[0].equalsIgnoreCase("scale") && scale <= Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            		if(logicalCond[0].equalsIgnoreCase("precision") && precision <= Integer.parseInt(logicalCond[2]))
            		{
            			type = destinationType;
            			break;
            		}
            			
            	}            	
            	
            }
            else if(sourceType.length == 2 && sourceType[1].equalsIgnoreCase(JSONConstants.ORC_DEFAULT_PREFIX))
            {
            	default_type = destinationType;
            }
     	}

     if (type.isEmpty()) {
            type = default_type;
     }
     return type;
}


	/** Get Date column position for source schema
	 * @param sourceSchema
	 * @param datatypeMap
	 * @return
	 */
	public static ArrayList<Integer> getDateColumnPosition(JSONObject sourceSchema,Map<String,String> datatypeMap) {
		JSONArray columnStructure;
		ArrayList<Integer> date_column_postion = new ArrayList<Integer>();
		String dataType = new String();
		try {
			columnStructure = sourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY);
			dataType = datatypeMap.get(Constants.DATE_COLUMN);
			String[] dataTypeArray = null;
			if(dataType !=  null) 
				dataTypeArray = dataType.split("\\,");
			if(dataTypeArray != null)
				{	
				for (int j = 0; j < columnStructure.length(); j++) {
					JSONObject column = (JSONObject) columnStructure.get(j);
					String type = column.getString(JSONConstants.COLUMN_TYPE);
					for(String dataTypes : dataTypeArray)
						{
							if(type.equalsIgnoreCase(dataTypes))
							{
								date_column_postion.add(Integer.parseInt(column.get(JSONConstants.COLUMN_SEQUENCE).toString())-1);
							}
						}
					}
				}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return date_column_postion;
	}

	/** Get Time column position for source schema 
	 * @param sourceSchema
	 * @param datatypeMap
	 * @return
	 */
	public static ArrayList<Integer> getTimeColumnPosition(JSONObject sourceSchema,Map<String,String> datatypeMap) {
		ArrayList<Integer> time_column_postion = new ArrayList<Integer>();
		JSONArray columnStructure;
		String dataType = new String();
		try {	
			columnStructure = sourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY);
			dataType = datatypeMap.get(Constants.TIME_COLUMN);
			String[] timeTypeArray = null;
			if(dataType !=  null)
				timeTypeArray = dataType.split("\\,");
			if(timeTypeArray != null)
			{
				for (int j = 0; j < columnStructure.length(); j++) {
					JSONObject column = (JSONObject) columnStructure.get(j);
					String type = column.getString(JSONConstants.COLUMN_TYPE);
					for(String dataTypes : timeTypeArray)
						{
							if(type.equalsIgnoreCase(dataTypes))
							{
								time_column_postion.add(Integer.parseInt(column.get(JSONConstants.COLUMN_SEQUENCE).toString())-1);
							}
						}
				}	
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return time_column_postion;
	}

	/**
	 * Get Length column position from given schema
	 * @param sourceSchema
	 * @param datatypeMap
	 * @return
	 */
	public static ArrayList<String> getLengthColumnPosition(JSONObject sourceSchema,Map<String,String> datatypeMap){
		ArrayList<String> length_column_position = new ArrayList<String>();
		JSONArray columnStructure;
		try{
			columnStructure = sourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY);
			String dataType = datatypeMap.get(Constants.LENGTH_COLUMN);
			String[] lengthTypeArray = null;
			if(dataType !=  null)
				lengthTypeArray = dataType.split("\\,");
			
			if(lengthTypeArray != null)
			{
				for (int j = 0; j < columnStructure.length(); j++) {
					JSONObject column = (JSONObject) columnStructure.get(j);
					Integer key = Integer.parseInt(column.get(JSONConstants.COLUMN_SEQUENCE).toString()) - 1;
					String type = column.getString(JSONConstants.COLUMN_TYPE);
					Integer length = ParserUtils.convertLengthForHive(type,Integer.parseInt(column.getString(JSONConstants.COLUMN_LENGTH)),datatypeMap);
						for(String dataTypes : lengthTypeArray)
						{
							if(type.equalsIgnoreCase(dataTypes))
							{
								length_column_position.add(key.toString() + ":" + length);
							}
						}
					}	
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return length_column_position;
	}	
	
	/** Generate JSON Array for header Information
     * @param HeaderColumnStructure
     * @return
     */
     public static JSONArray getHeaderJsonArray(
                  ArrayList<ColumnStructure> HeaderColumnStructure) {
           JSONArray tempArray = new JSONArray();
           try {
                  for (ColumnStructure columnStructureList : HeaderColumnStructure) {
                         JSONObject dblJSON = new JSONObject();
                         dblJSON.put(JSONConstants.COLUMN_NAME, columnStructureList.getName());
                         dblJSON.put(JSONConstants.COLUMN_TYPE, columnStructureList.getType());
                         dblJSON.put(JSONConstants.COLUMN_LENGTH, columnStructureList.getLength());
                         dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
                         dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
                         if(!columnStructureList.getNullable())
                                dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
                         else   
                                dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
                         dblJSON.put(JSONConstants.COLUMN_SEQUENCE, columnStructureList.getSeq());
                         tempArray.put(dblJSON);
                  }
           } catch (JSONException e) {
                  e.printStackTrace();
           }
           return tempArray;
     }
     
 	/** Generate JSON Array for footer Information
      * @param FooterColumnStructure
      * @return
      */
      public static JSONArray getFooterJsonArray(
                   ArrayList<ColumnStructure> FooterColumnStructure,JSONArray jArray) {
            
    	  try {
                   for (ColumnStructure columnStructureList : FooterColumnStructure) {
                          JSONObject dblJSON = new JSONObject();
                          dblJSON.put(JSONConstants.COLUMN_NAME, columnStructureList.getName());
                          dblJSON.put(JSONConstants.COLUMN_TYPE, columnStructureList.getType());
                          dblJSON.put(JSONConstants.COLUMN_LENGTH, columnStructureList.getLength());
                          dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
                          dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
                          if(!columnStructureList.getNullable())
                                 dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
                          else   
                                 dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
                          dblJSON.put(JSONConstants.COLUMN_SEQUENCE, columnStructureList.getSeq());
                          jArray.put(dblJSON);
                   }
            } catch (JSONException e) {
                   e.printStackTrace();
            }
            return jArray;
      }

	

	/**
	 * cleanString remove tab newline from string
	 * 
	 * @param tempString
	 *            unformatted String contain tab newline
	 * @return String : Formatted String
	 */
	public static String cleanString(String tempString) {

		tempString = tempString.replaceAll("[\\t\\n\\r]","");
		tempString = tempString.replace("\"", "");
		tempString = tempString.trim();
		tempString = tempString.toLowerCase();
		return tempString;
	}
	
	/**
	 * Remove control character from given string 
	 * @param tempString
	 * @return
	 */
	public static String removeControlChar(String tempString) {

		tempString = tempString.replaceAll("[\\t\\n\\r\\p{C}\\p{Z}]","");
		tempString = tempString.trim();
		tempString = tempString.toLowerCase();
		return tempString;
	}
	
	/** Generate Transformation schema JSON Object
	 * @param propertiesMap
	 * @return
	 */
	public static JSONObject getTransformationsSchema(
			Map<String, String> propertiesMap) {

		JSONObject transformationsSchema = new JSONObject();
		try {
			transformationsSchema.put("tgtdateformat",
					propertiesMap.get(Constants.TARGET_DATE_FORMAT));
			transformationsSchema.put("tgttimeformat",
					propertiesMap.get(Constants.TARGET_TIME_FORMAT));
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return transformationsSchema;
	}

	
	/**
	 * @param datatypeMap : Datatype mapping file content
	 * @return LinkedHashMap<String, Map<String, String>> : Map of Datatype which has avro_ as prefix 
	 */
	public static LinkedHashMap<String, Map<String, String>> getAvroTypeConversion(Map<String,String> datatypeMap) {
		LinkedHashMap<String, Map<String, String>> avroDataTypeMap = new LinkedHashMap<String, Map<String, String>>();
		for (Map.Entry<String, String> entry : datatypeMap.entrySet())
		{
			String[] sourceType= entry.getKey().split(JSONConstants.SEPERATOR);
			if(sourceType.length > 1)
			{
				String orcDataType = cleanString(sourceType[0].toString());
				String dataTypeParam = cleanString(sourceType[1].toString());
				String avroDataType= entry.getValue().toString().toLowerCase();	
				if(orcDataType.matches(JSONConstants.AVRO_DATATYPE_PREFIX + "(.*)"))
				{
					orcDataType = orcDataType.replaceAll(JSONConstants.AVRO_DATATYPE_PREFIX, "");
					Map<String, String> dataTypeMap = new HashMap<String, String>();
					if(!JsonUtils.isKeyExists(avroDataTypeMap, orcDataType))
					{
						avroDataTypeMap.put(orcDataType, dataTypeMap);
					}
					else
					{
						dataTypeMap = avroDataTypeMap.get(orcDataType);
					}
					dataTypeMap.put(dataTypeParam, avroDataType);
				}
			}
		}	
		return avroDataTypeMap;
	}
	
	/** Generate updated meta info JSON with new attributes
	 * @param metaInformation - meta info JSON object 
	 * @param srcRowCount
	 * @param date_column_position
	 * @param time_column_position
	 * @param length_column_position_and_value
	 * @param propertiesMap
	 * @param sourceSchema
	 * @return
	 */
	public static JSONObject getModifiedMetaInformation(JSONObject metaInformation,Integer srcRowCount,ArrayList<Integer> date_column_position,ArrayList<Integer> time_column_position,ArrayList<String> length_column_position_and_value,Map<String,String> propertiesMap,JSONObject sourceSchema) {
		JSONObject metInformationtemp = null;
		try {
			metInformationtemp = new JSONObject(metaInformation.toString());
			metInformationtemp.put(JSONConstants.SOURCE_COLUMN_COUNT, srcRowCount);
			JSONArray jArraydate_column_position = new JSONArray();
			if (!date_column_position.isEmpty()) {
				for (int i = 0; i < date_column_position.size(); i++) {
					jArraydate_column_position.put(date_column_position.get(i));
				}
			}
			metInformationtemp.put(JSONConstants.SOURCE_DATE_COLUMN_POSITION,jArraydate_column_position);

			JSONArray jArraytime_column_position = new JSONArray();
			if (!time_column_position.isEmpty()) {
				for (int i = 0; i < time_column_position.size(); i++) {
					jArraytime_column_position.put(time_column_position.get(i));
				}
			}
			
			metInformationtemp.put(JSONConstants.SOURCE_TIME_COLUMN_POSITION,jArraytime_column_position);

			JSONArray jArraylength_column_position = new JSONArray();
			if (!length_column_position_and_value.isEmpty()) {
				for (int i = 0; i < length_column_position_and_value.size(); i++) {
					jArraylength_column_position.put(length_column_position_and_value.get(i));
				}
			}
			metInformationtemp.put(JSONConstants.SOURCE_COLUMN_LENGTH_POSITION_VALUE,jArraylength_column_position);
			
			if(metaInformation.has("srctype")){
				String systemType = metaInformation.getString("srctype");
				if(systemType.equalsIgnoreCase("CDC")){
					JSONArray columnStructure = sourceSchema.getJSONArray(JSONConstants.COLUMNS_ARRAY);
					for (int j = 0; j < columnStructure.length(); j++) {
						JSONObject column = (JSONObject) columnStructure.get(j);
						String name = column.getString(JSONConstants.COLUMN_NAME);
						if(name.equalsIgnoreCase("c_userid"))
							metInformationtemp.put("fdposition", (Integer.parseInt(column.get(JSONConstants.COLUMN_SEQUENCE).toString())-1));
					}
					metInformationtemp.put("fdvalue",propertiesMap.get(Constants.FDVALUE));
					metInformationtemp.put("fdflag", propertiesMap.get(Constants.FDFLAG));
				}				
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return metInformationtemp;
	}
	
	/**
	 * creates ORC table schema query
	 * @param jsonDestinationschemaArray
	 * @return
	 */
	public static String createORCtableSchemaQuery(JSONArray jsonDestinationschemaArray,String partition_col){
		StringBuilder query = new StringBuilder();
		Gson gson = new Gson();
		String query_string = new String();
		try{
		for (int i = 0; i < jsonDestinationschemaArray.length(); i++) {	
			HiveColumnStructure hiveColumnStructure = new HiveColumnStructure();
			hiveColumnStructure = gson.fromJson(jsonDestinationschemaArray.get(i).toString(), HiveColumnStructure.class); 
			if(hiveColumnStructure.getColname().equalsIgnoreCase(partition_col)){
				continue;
			}
			query.append("`" + hiveColumnStructure.getColname() + "`" + " " + hiveColumnStructure.getColtype());
			if(hiveColumnStructure.getColtype().contains("decimal")){
				query.append("(" + hiveColumnStructure.getColprecision() + "," + hiveColumnStructure.getColscale() + ")" );
			}
			if(hiveColumnStructure.getColtype().contains("varchar"))
				query.append("(" + hiveColumnStructure.getCollen()+ ")");
			query.append(",\n");
		}
		query_string = query.substring(0, query.length()-2);
		}catch(JSONException e){
			log.error(e.getMessage());
		}
		return query_string;
	}
	
	/**
	 * creates TEXT table schema query
	 * @param jsonDestinationschemaArray
	 * @return
	 */
	public static JSONArray createTempTexttableSchemaQuery(JSONArray jsonSourceschemaArray,JSONArray jsonDestinationschemaArray, JSONArray partitionColArray){
		StringBuilder query = new StringBuilder();
		Gson gson = new Gson();
		JSONArray finalArray = new JSONArray();
		try{
			for(int i = 0; i < jsonSourceschemaArray.length();i++){
				HiveColumnStructure hiveColumnStructure = new HiveColumnStructure();
				hiveColumnStructure = gson.fromJson(jsonSourceschemaArray.get(i).toString(), HiveColumnStructure.class); 
				String srcCol = hiveColumnStructure.getColname();
				boolean is_found = false;
				for(int j = 0; j < jsonDestinationschemaArray.length();j++){
					HiveColumnStructure deshiveColumnStructure = new HiveColumnStructure();
					deshiveColumnStructure = gson.fromJson(jsonDestinationschemaArray.get(j).toString(), HiveColumnStructure.class); 
					String desCol = deshiveColumnStructure.getColname();
					if(srcCol.equalsIgnoreCase(desCol)){
						finalArray.put(jsonDestinationschemaArray.get(j)); 
						is_found = true;
						break;
					}				
				}
				if(!is_found){
					for(int k = 0; k < partitionColArray.length();k++){
						HiveColumnStructure parthiveColumnStructure = new HiveColumnStructure();
						parthiveColumnStructure = gson.fromJson(partitionColArray.get(k).toString(), HiveColumnStructure.class); 
						String desCol = parthiveColumnStructure.getColname();
						if(srcCol.equalsIgnoreCase(desCol)){
							finalArray.put(partitionColArray.get(k)); 
							break;
						}				
					}
				}
			}
		}catch(JSONException e){
			log.error(e.getMessage());
		}
		
		
		return finalArray;
	}
	
	/**
	 * creates TEXT table schema query
	 * @param jsonDestinationschemaArray
	 * @return
	 */
	public static String createTexttableSchemaQuery(JSONArray jsonDestinationschemaArray,String partition_col){
		StringBuilder query = new StringBuilder();
		Gson gson = new Gson();
		String query_string = new String();
		try{
		for (int i = 0; i < jsonDestinationschemaArray.length(); i++) {	
			HiveColumnStructure hiveColumnStructure = new HiveColumnStructure();
			hiveColumnStructure = gson.fromJson(jsonDestinationschemaArray.get(i).toString(), HiveColumnStructure.class); 
			if(hiveColumnStructure.getColname().equalsIgnoreCase(partition_col)){
				continue;
			}
			query.append("`" + hiveColumnStructure.getColname() + "`" + " " + hiveColumnStructure.getColtype());
			if(hiveColumnStructure.getColtype().contains("decimal")){
				query.append("(" + hiveColumnStructure.getColprecision() + "," + hiveColumnStructure.getColscale() + ")" );
			}
			if(hiveColumnStructure.getColtype().contains("varchar"))
				query.append("(" + hiveColumnStructure.getCollen()+ ")");
			query.append(",\n");
		}
		query_string = query.substring(0, query.length()-2);
		}catch(JSONException e){
			log.error(e.getMessage());
		}
		return query_string;
	}
}
