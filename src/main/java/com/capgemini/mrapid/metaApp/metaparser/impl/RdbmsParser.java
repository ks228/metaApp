/**
 * 
 */
package com.capgemini.mrapid.metaApp.metaparser.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.integration.impl.MysqlHandler;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.Generator;
import com.capgemini.mrapid.metaApp.utils.Decrypt;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;

/**
 * @author audasi
 *
 */
public class RdbmsParser implements IParser {

	final static Logger log = Logger.getLogger(RdbmsParser.class);

	public static ArrayList<Integer> date_column_position;
	public static ArrayList<Integer> time_column_position;
	public static ArrayList<String> length_column_position_and_value;
	public static Integer srcSeqCount, srcRowCount, descSeqCount;
	public static Map<String, String> tablePKValues = null;
	public static String tableName;

	
	public JSONObject createJsonfromSchema(ArrayList<String> schema,
			JSONObject metaInformation, Map<String, String> propertiesMap,
			String srcSystem, String country, String table,
			Map<String, String> datatypeMap, Workbook workbook,
			Map<String, String> excelPropertyMap, String tableClassification) {

		date_column_position = new ArrayList<Integer>();
		time_column_position = new ArrayList<Integer>();
		length_column_position_and_value = new ArrayList<String>();
		srcSeqCount = 1;
		srcRowCount = 0;
		descSeqCount = 1;
		JSONObject rdbmsFinalJSONObject = new JSONObject();
		try{
			String decryptPassword = Decrypt.decrypt(propertiesMap.get(Constants.MYSQL_PASSWORD), Generator.secretKey);
			MysqlHandler mysqlHandlerDevOps = new MysqlHandler(propertiesMap,propertiesMap.get(Constants.SOURCE_METAAPP_DEV_OPS),decryptPassword);
			Connection MetaAppDevOpsConnection = mysqlHandlerDevOps.getConnection();
			log.info("Connection created");
			ArrayList<ColumnStructure> RDBMSColumnStructure = new ArrayList<ColumnStructure>();
			RDBMSColumnStructure = fetchTableSchemaInfo(MetaAppDevOpsConnection,srcSystem,table);
			mysqlHandlerDevOps.closeConnection(MetaAppDevOpsConnection);
			log.info("Connection closed");
			JSONObject rdbmsSourceSchema = getSourceSchema(RDBMSColumnStructure);
			JSONObject rdbmsDestinationSchema = getDestinationSchema(RDBMSColumnStructure,datatypeMap,propertiesMap,metaInformation);
			JSONObject rdbmsTransformationsSchema = ParserUtils.getTransformationsSchema(propertiesMap);
			log.info("JSON Created");
			date_column_position = ParserUtils.getDateColumnPosition(rdbmsSourceSchema,datatypeMap);
			time_column_position = ParserUtils.getTimeColumnPosition(rdbmsSourceSchema,datatypeMap);
			length_column_position_and_value = ParserUtils.getLengthColumnPosition(rdbmsSourceSchema,datatypeMap);
		
			JSONObject modifiedMetaInformationJsonObject = ParserUtils.getModifiedMetaInformation(metaInformation, srcRowCount,date_column_position, time_column_position,length_column_position_and_value, propertiesMap,rdbmsSourceSchema);

			rdbmsFinalJSONObject.put(JSONConstants.SOURCE_SCHEMA, rdbmsSourceSchema);
			rdbmsFinalJSONObject.put(JSONConstants.DESTINATION_SCHEMA,rdbmsDestinationSchema);
			rdbmsFinalJSONObject.put(JSONConstants.TRANSFORMATION_SCHEMA,rdbmsTransformationsSchema);
			rdbmsFinalJSONObject.put(JSONConstants.METAINFORMATION_SCHEMA,modifiedMetaInformationJsonObject);		
		} catch(Exception e){
			e.printStackTrace();
			return null;
		}
		return rdbmsFinalJSONObject;
	}

	private ArrayList<ColumnStructure> fetchTableSchemaInfo(Connection conn,String srcSystem,String table){
		
		ArrayList<ColumnStructure> sourceSchema = new ArrayList<ColumnStructure>();
		Statement stmt = null;
		try{
		
			stmt = conn.createStatement();
			ResultSet res = stmt.executeQuery("SELECT SRC_NAME,SRC_TABLE_NAME,SRC_COLUMN_NAME,SRC_COLUMN_DATATYPE,SRC_COLUMN_NULL,SRC_COLUMN_PK,SRC_COLUMN_LENGTH,SRC_COLUMN_PRECISION,SRC_COLUMN_SCALE,SRC_COLUMN_DEFAULT,SRC_COLUMN_COMMENTS FROM COLUMN_DETAILS WHERE SRC_NAME='"+srcSystem+"' AND SRC_TABLE_NAME='"+table+"'");
		
			while(res.next()){
				ColumnStructure colInfo = new ColumnStructure();
				colInfo.setColumnName(res.getString("SRC_COLUMN_NAME"));
				colInfo.setDataType(res.getString("SRC_COLUMN_DATATYPE"));
				colInfo.setLength(res.getInt("SRC_COLUMN_LENGTH"));
				colInfo.setPrecision(res.getInt("SRC_COLUMN_PRECISION"));
				colInfo.setScale(res.getInt("SRC_COLUMN_SCALE"));
				if(res.getString("SRC_COLUMN_NULL").equalsIgnoreCase("YES"))
					colInfo.setNullable(true);
				else
					colInfo.setNullable(false);
				if(res.getString("SRC_COLUMN_PK").equalsIgnoreCase("pri"))
					colInfo.setKey(true);
				else
					colInfo.setKey(false);
				sourceSchema.add(colInfo);
			}	
		}catch(Exception e){
			System.out.println(e.getMessage());
		}finally{
			
		}
		return sourceSchema;
	}
	
	private JSONObject getSourceSchema(
			ArrayList<ColumnStructure> ColumnStructure) {
		ArrayList<Integer> src_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> src_primary_key = new ArrayList<String>();

		JSONObject jObject = new JSONObject();
		try {
			JSONArray jArray = new JSONArray();
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME, columnStructureList.getName());
				dblJSON.put(JSONConstants.COLUMN_TYPE, columnStructureList.getType());
				dblJSON.put(JSONConstants.COLUMN_LENGTH, columnStructureList.getLength());
				dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, srcSeqCount);	
				if(!columnStructureList.getNullable()){
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
					if(columnStructureList.getKey()){
						src_primary_key.add(columnStructureList.getName());
						src_primary_key_position.add(srcSeqCount-1);
					}
				}else{
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
				}
				srcSeqCount++;
				srcRowCount++;
				jArray.put(dblJSON);
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!src_primary_key.isEmpty()) {
				for (String primaryKey : src_primary_key) {
					jArrayPrimaryKey.put(primaryKey);
				}
			}	

			JSONArray jKeyArray = new JSONArray();
			if(src_primary_key_position.size() > 0){
				String keyPositions = StringUtils.join(src_primary_key_position, ',');
				jKeyArray.put(keyPositions);
			}

			jObject.put(JSONConstants.COLUMNS_ARRAY, jArray);	
			jObject.put(JSONConstants.PRIMARY_KEY, jArrayPrimaryKey);
			jObject.put(JSONConstants.PRIMARY_KEY_POSITION, jKeyArray);
		} catch (JSONException jse) {
			jse.printStackTrace();
		}
		return jObject;
	}
	
	
	private JSONObject getDestinationSchema(
			ArrayList<ColumnStructure> ColumnStructure,Map<String,String> datatypeMap,Map<String,String> propertiesMap,JSONObject metaInformation) {
		JSONObject jObject = new JSONObject();
		ArrayList<Integer> desc_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> desc_primary_key = new ArrayList<String>();

		try {
			JSONArray jArray = new JSONArray();
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME, ParserUtils.removeControlChar(columnStructureList.getName()));

				String orcDataType = ParserUtils.convertDataTypeForHive(columnStructureList.getType(), datatypeMap,columnStructureList.getPrecision(),columnStructureList.getScale());

				dblJSON.put(JSONConstants.COLUMN_TYPE, orcDataType);
				dblJSON.put(JSONConstants.COLUMN_LENGTH,ParserUtils.convertLengthForHive(columnStructureList.getType(),columnStructureList.getLength(), datatypeMap));

				dblJSON.put(JSONConstants.COLUMN_PRECISION,columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, descSeqCount);
				if(!columnStructureList.getNullable()){
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
					if(columnStructureList.getKey()){
						desc_primary_key.add(columnStructureList.getName());
						desc_primary_key_position.add(descSeqCount-1);
					}
				}else{
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE); 
				}
				jArray.put(dblJSON);
				descSeqCount++;				
			}
			
			
			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!desc_primary_key.isEmpty()) {
				for (String primaryKey : desc_primary_key) {
					jArrayPrimaryKey.put(primaryKey);
				}
			}	

			JSONArray jKeyArray = new JSONArray();
			if(desc_primary_key_position.size() > 0){
				String keyPositions = StringUtils.join(desc_primary_key_position, ',');
				jKeyArray.put(keyPositions);
			}

			jObject.put(JSONConstants.COLUMNS_ARRAY, jArray);	
			jObject.put(JSONConstants.PRIMARY_KEY, jArrayPrimaryKey);
			jObject.put(JSONConstants.PRIMARY_KEY_POSITION, jKeyArray);
		} catch (JSONException jse) {
			jse.printStackTrace();
		}
		return jObject;		
	}
}
