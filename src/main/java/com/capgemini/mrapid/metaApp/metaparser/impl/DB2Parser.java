package com.capgemini.mrapid.metaApp.metaparser.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.exceptions.FileFormatException;
import com.capgemini.mrapid.metaApp.exceptions.FileParsingError;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;

/**
 * @author Anurag Udasi
 *
 * Class DB2Parser : Parser class to
 * 1.create DB2 DDL file to JSON,
 * 2.parse/convert json string to avsc, write to HDFS,
 * 3.parse/convert json string to hive ddl - hql file, write to HDFS
 */

public class DB2Parser implements IParser {
	final static Logger log = Logger.getLogger(DB2Parser.class);
	public static ArrayList<String> primary_key;
	public static ArrayList<Integer> date_column_position;
	public static ArrayList<Integer> time_column_position;
	public static ArrayList<String> length_column_position_and_value;
	public static Integer srcSeqCount, srcRowCount, descSeqCount;
	
	/**
	 * createJsonfromSchema create JSON from schema, here DB2 DLL
	 * @param ddlList
	 * @param metaInformation : JSONObject contains Meta level information
	 * @param propertiesMap : Hadoop configuration map
	 * @param srcSystem : srcSystem for which operation is done
	 * @param country : country for which operation is done
	 * @param table : table for which operation is done
	 * @param datatypeMap : Contains Data Type Mapping for DB Type coming from properties file
	 * @param workbook : needed if schema type is excel
	 * @param excelPropertyMap : needed if schema type is excel
	 * @param classification
	 * @return JSONObject: Contain SourceSchema DestinationSchema
	 */

	public JSONObject createJsonfromSchema(ArrayList<String> ddlList,
			JSONObject metaInformation, Map<String, String> propertiesMap,
			String srcSystem, String country, String table, Map<String,String> datatypeMap,Workbook workbook,
			Map<String,String> excelPropertyMap, String classification) {
		JSONObject ddlFinalJSONObject = new JSONObject();
		srcSeqCount = 1;
		srcRowCount = 0;
		descSeqCount = 1;
		primary_key = new ArrayList<String>();
		date_column_position = new ArrayList<Integer>();
		time_column_position = new ArrayList<Integer>();
		length_column_position_and_value = new ArrayList<String>();
		
		try {
			if(ddlList.size() > 2)
				throw new FileFormatException("DDL file is not in standard Format...!");
			
			ArrayList<ColumnStructure> ddlSrcHeaderColumnStructure = getSrcHeaderColumns(propertiesMap,metaInformation);
			ArrayList<ColumnStructure> ddlSescHeaderColumnStructure = getDescHeaderColumns(propertiesMap,metaInformation);
			ArrayList<ColumnStructure> ddlColumnStructure = getSourceStructure(ddlList.get(0).toString());
			
			if (ddlList.size() > 1) {
				primary_key = getPrimaryKey(ddlList.get(1).toString());
			}
			
		
			JSONObject ddlSourceSchema = getSourceSchema(ddlColumnStructure,ddlSrcHeaderColumnStructure);
			JSONObject ddlDestinationSchema = getDestinationSchema(ddlColumnStructure, ddlSescHeaderColumnStructure, datatypeMap);
			JSONObject ddlTransformationsSchema = ParserUtils.getTransformationsSchema(propertiesMap);
		
			date_column_position = ParserUtils.getDateColumnPosition(ddlSourceSchema,datatypeMap);
			time_column_position = ParserUtils.getTimeColumnPosition(ddlSourceSchema,datatypeMap);
			length_column_position_and_value = ParserUtils.getLengthColumnPosition(ddlSourceSchema,datatypeMap);
	
			JSONObject modifiedMetaInformationJsonObject = ParserUtils.getModifiedMetaInformation(metaInformation,srcRowCount,date_column_position,time_column_position,length_column_position_and_value,propertiesMap,ddlSourceSchema);
					
			ddlFinalJSONObject.put(JSONConstants.SOURCE_SCHEMA, ddlSourceSchema);
			ddlFinalJSONObject.put(JSONConstants.DESTINATION_SCHEMA, ddlDestinationSchema);
			ddlFinalJSONObject.put(JSONConstants.TRANSFORMATION_SCHEMA, ddlTransformationsSchema);
			ddlFinalJSONObject.put(JSONConstants.METAINFORMATION_SCHEMA,
					modifiedMetaInformationJsonObject);

		} catch (JSONException e) {
			log.error(e.getMessage());
			return null;
		} catch (FileParsingError e){
			log.error(e.getMessage());
			return null;
		} catch(FileFormatException e){
			log.error(e.getMessage());
			return null;
		}
		return ddlFinalJSONObject;
	}


	/**
	 * getDestinationSchema: extract Destination Schema
	 * @param ColumnStructure : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @param datatypeMap : Contains Data Type Mapping for DB Type coming from properties file
	 * @return JSONObject : DestinationSchema in json format
	 */
	private JSONObject getDestinationSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure,
			Map<String,String> datatypeMap) {
		JSONObject jObject = new JSONObject();
		ArrayList<Integer> desc_primary_key_position = new ArrayList<Integer>();
				
		try {
			JSONArray jArray = new JSONArray();
			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				String fieldName = columnStructureList.getName();
				dblJSON.put(JSONConstants.COLUMN_NAME, ParserUtils.cleanString(columnStructureList.getName()));
				dblJSON.put(JSONConstants.COLUMN_TYPE,	ParserUtils.convertDataTypeForHive(columnStructureList.getType(),datatypeMap,columnStructureList.getPrecision(),columnStructureList.getScale()));
				dblJSON.put(JSONConstants.COLUMN_LENGTH, ParserUtils.convertLengthForHive(columnStructureList.getType(),columnStructureList.getLength(),datatypeMap));
				dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				if(!columnStructureList.getNullable())
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
				else	
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
				
				if (!columnStructureList.getDefault().equalsIgnoreCase(
						"9999999999"))
					dblJSON.put(JSONConstants.COLUMN_DEFAULT, columnStructureList.getDefault());
				
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, descSeqCount);
				
				if(primary_key.contains(fieldName))
				{
					desc_primary_key_position.add(descSeqCount - 1);
				}
				jArray.put(dblJSON);
				descSeqCount++;
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!primary_key.isEmpty()) {
				for (String primaryKey : primary_key) {
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

	/**
	 * getSourceSchema: extract Source Schema
	 * @param ColumnStructure
	 *            : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @return JSONObject : SourceSchema in json format
	 */
	private JSONObject getSourceSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure) {

		JSONObject jObject = new JSONObject();
		ArrayList<Integer> src_primary_key_position = new ArrayList<Integer>();
		
		try {
			JSONArray jArray = new JSONArray();
			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				String fieldName = columnStructureList.getName();
				dblJSON.put(JSONConstants.COLUMN_NAME, columnStructureList.getName());
				dblJSON.put(JSONConstants.COLUMN_TYPE, columnStructureList.getType());
				dblJSON.put(JSONConstants.COLUMN_LENGTH, columnStructureList.getLength());
				dblJSON.put(JSONConstants.COLUMN_PRECISION, columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE, columnStructureList.getScale());
				if(!columnStructureList.getNullable())
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_TRUE);
				else	
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,JSONConstants.NOT_NULL_FALSE);
				if (!columnStructureList.getDefault().equalsIgnoreCase(
						"9999999999"))
					dblJSON.put(JSONConstants.COLUMN_DEFAULT, columnStructureList.getDefault());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, srcSeqCount);
				if(primary_key.contains(fieldName))
				{
					src_primary_key_position.add(srcSeqCount - 1);
				}
				srcSeqCount++;
				srcRowCount++;
				jArray.put(dblJSON);
			}
			JSONArray jArrayPrimaryKey = new JSONArray();
			if (!primary_key.isEmpty()) {
				for (String primaryKey : primary_key) {
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

	/**Generate Array list of header columns for source schema
	 * @param propertiesMap
	 * @param metaInformation
	 * @return
	 */
	private ArrayList<ColumnStructure> getSrcHeaderColumns(
			Map<String, String> propertiesMap, JSONObject metaInformation) {

		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();

		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(
				propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true,true);
		headerStructure.add(uniqueIdStructure);

		//Add CDC columns if srcType is CDC
		if(metaInformation.has("srctype")){
			try {
				 String systemType = metaInformation.getString("srctype");
				 if(systemType.equalsIgnoreCase("CDC")){
					String[] cdcColumn = propertiesMap.get(Constants.CDC_COULMN).split(
							"\\,");
					for (int i = 0; i < cdcColumn.length; i++) {
						ColumnStructure cdcColStructure = new ColumnStructure();
						cdcColStructure = getHeaderColumnStructure(cdcColumn[i], false,true);
						headerStructure.add(cdcColStructure);
					}
				}
			} catch (JSONException e) {
				log.error(e.getMessage());
			}			
		}
		return headerStructure;
	}
	
	/** Generate column structure for source header columns
	 * @param columnName - column name
	 * @param not_null - flag for not null check
	 * @param is_source
	 * @return
	 */
	private ColumnStructure getHeaderColumnStructure(String columnName,Boolean not_null,Boolean is_source) {
		ColumnStructure headerColumnStructure = new ColumnStructure();
		headerColumnStructure.setName(columnName);
		headerColumnStructure.setType("string");
		headerColumnStructure.setLength(0);
		headerColumnStructure.setPrecision(0);
		headerColumnStructure.setScale(0);
		if (not_null)
			headerColumnStructure.setNullable(false);
		else
			headerColumnStructure.setNullable(true);
		
		if(is_source){
			headerColumnStructure.setSeq(srcSeqCount);
			srcRowCount++;
			srcSeqCount++;
		}else{
			headerColumnStructure.setSeq(descSeqCount);
			descSeqCount++;
		}
		return headerColumnStructure;
	}

	/**Generate Array list of header columns for destination schema  
	 * @param propertiesMap
	 * @param metaInformation
	 * @return
	 */
	private ArrayList<ColumnStructure> getDescHeaderColumns(
			Map<String, String> propertiesMap, JSONObject metaInformation) {
		
		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();
		
		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(
				propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true,false);
		headerStructure.add(uniqueIdStructure);
		
		//Add CDC columns if srcType is CDC
		String systemType = "";
		if(metaInformation.has("srctype")){
			try {
				systemType = metaInformation.getString("srctype");
				if(systemType.equalsIgnoreCase("CDC")){
					String[] cdcColumn = propertiesMap.get(Constants.CDC_COULMN).split(
							"\\,");
					for (int i = 0; i < cdcColumn.length; i++) {
						ColumnStructure cdcColStructure = new ColumnStructure();
						cdcColStructure = getHeaderColumnStructure(cdcColumn[i],false,false);
						headerStructure.add(cdcColStructure);
					}
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}			
		}
		return headerStructure;
	}

	/** Generate column structure array list for source schema
	 * @param ddlObject - schema structure as string
	 * @return
	 */
	private ArrayList<ColumnStructure> getSourceStructure(String ddlObject) throws FileParsingError {

		String[] tempddlString = ddlObject.split("\\,\\s*\\n");
		ArrayList<ColumnStructure> columnStructureList = new ArrayList<ColumnStructure>();
		for (int i = 0; i < tempddlString.length; i++) {
			ColumnStructure colStructure = new ColumnStructure();
			String ddlString = ParserUtils.cleanString(tempddlString[i].toString());
			if (ddlString.contains("create table")) {

				colStructure = getFirstColumn(ddlString);
				columnStructureList.add(colStructure);

			} else if (ddlString.contains(";")) {

				colStructure = getLastColumn(ddlString);
				columnStructureList.add(colStructure);
			} else {

				colStructure = getColumnInfo(ddlString);
				columnStructureList.add(colStructure);
			}
		}
		return columnStructureList;
	}

	/**
	 * getFirstColumn :getting first Column from create statement of DB2 DDL
	 * 
	 * @param ddlStringObject
	 *            :DB2 DDL String
	 * @return ColumnStructure : ColumnStructure object contains column
	 *         information
	 * @throws FileParsingError 
	 */
	private ColumnStructure getFirstColumn(String ddlStringObject) throws FileParsingError {
		ColumnStructure colStructure = new ColumnStructure();
		String pattern = "create table\\s+\\S+\\s*\\((.*)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(ddlStringObject);
		String tempString = "";
		if (m.find()) {
			tempString = m.group(1);
			tempString = ParserUtils.cleanString(tempString);
		} else {
			throw new FileParsingError("Fail to parse DDL file..!");
		}
		colStructure = getColumnInfo(tempString);
		return colStructure;
	}

	/**
	 * getColumnInfo :getting column level information(default,NULLABLE) from
	 * DB2 DDL
	 * 
	 * @param ddlStringObject
	 *            :DB2 DDL String
	 * @return ColumnStructure : ColumnStructure object contains column level
	 *         (default,NULLABLE)information
	 */
	private ColumnStructure getColumnInfo(String ddlStringObject) {
		ColumnStructure colStructure = new ColumnStructure();
		String[] fst_column = ddlStringObject.split(" ");
		if (fst_column.length > 2) {
			if (ddlStringObject.contains("not null")
					&& ddlStringObject.contains("default")) {
				colStructure = getColumnStructureObject(ddlStringObject, true,
						true);
			} else if (ddlStringObject.contains("not null")) {
				colStructure = getColumnStructureObject(ddlStringObject, true,
						false);
			} else if (ddlStringObject.contains("default")) {
				colStructure = getColumnStructureObject(ddlStringObject, false,
						true);
			} else {
				colStructure = getColumnStructureObject(ddlStringObject, false,
						false);
			}
		} else {
			colStructure = getColumnStructureObject(ddlStringObject, false,
					false);
		}
		return colStructure;
	}

	/**
	 * getPrimaryKey :getting Primary key informations from DB2 Primary key	 * 
	 * @param tempPrimaryKeyString
	 *            :DB2 Primary key String
	 * @return ArrayList : ArrayList contains all primary keys of table
	 * @throws FileParsingError 
	 */
	private ArrayList<String> getPrimaryKey(String tempPrimaryKeyString) throws FileParsingError {
		tempPrimaryKeyString = ParserUtils.cleanString(tempPrimaryKeyString);
		ArrayList<String> primary_key = new ArrayList<String>();
		String pattern = "primary\\s+key\\s*\\((.*)\\)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(tempPrimaryKeyString.toString());
		if (m.find()) {
			if (m.group(1).contains(",")) {
				String[] tempPrimarykey = m.group(1).split("\\,");
				for (int i = 0; i < tempPrimarykey.length; i++) {
					primary_key.add(tempPrimarykey[i].toString().trim());
				}
			} else {
				primary_key.add(m.group(1));
			}
		} else {
			throw new FileParsingError("Error While Parsing Primary Key...!");
		}
		return primary_key;
	}

	/**
	 * getColumnStructureObject :getting ColumnStructuresuch as decimal column
	 * @param columndata
	 *            : String Column information with datatype
	 * @param null_flag
	 * @param default_flag
	 * @return ColumnStructure : ColumnStructure object with type Precision
	 *         scale Nullable values
	 */
	private ColumnStructure getColumnStructureObject(String columndata,
			boolean null_flag, boolean default_flag) {
		ColumnStructure colStructure = new ColumnStructure();
		String[] fst_column = columndata.split(" ");
		colStructure.setName(fst_column[0].toString());
		colStructure.setType(getDataType(fst_column[1].toString()));
		if (fst_column[1].toString().contains("decimal")) {
			colStructure.setPrecision(getPrecision(fst_column[1].toString()));
			colStructure.setScale(getScale(fst_column[1].toString()));
			colStructure.setLength(0);
		} else {
			colStructure.setLength(getLength(fst_column[1].toString()));
			colStructure.setPrecision(0);
			colStructure.setScale(0);
		}

		if (null_flag && default_flag) {
			colStructure.setNullable(false);
			if (fst_column.length < 7)
				colStructure.setDefault("9999999999");
			else {
				if (fst_column[6].toString().equalsIgnoreCase("''"))
					colStructure.setDefault("");
				else
					colStructure.setDefault(fst_column[6].toString());
			}
		} else if (null_flag) {
			colStructure.setNullable(false);
		} else if (default_flag) {
			if (fst_column.length < 5)
				colStructure.setDefault("9999999999");
			else {
				if (fst_column[4].toString().equalsIgnoreCase("''"))
					colStructure.setDefault("");
				else
					colStructure.setDefault(fst_column[4].toString());
			}
			colStructure.setNullable(true);
		} else {
			colStructure.setNullable(true);
		}
		return colStructure;

	}

	/**
	 * getLastColumn :getting last Column from create statement of DB2 DDL
	 * 
	 * @param ddlStringObject
	 *            :DB2 DDL String
	 * @return ColumnStructure : ColumnStructure object contains column
	 *         information
	 */
	private ColumnStructure getLastColumn(String ddlStringObject) {
		ColumnStructure colStructure = new ColumnStructure();
		String[] lst_column = ddlStringObject.split("\\)");
		if (lst_column.length > 2) {
			if (ddlStringObject.contains("not null")
					&& ddlStringObject.contains("default")) {
				colStructure = getColumnStructureObject(ddlStringObject, true,
						true);
			} else if (ddlStringObject.contains("not null")) {
				colStructure = getColumnStructureObject(ddlStringObject, true,
						false);
			} else if (ddlStringObject.contains("default")) {
				colStructure = getColumnStructureObject(ddlStringObject, false,
						true);
			} else {
				colStructure = getColumnStructureObject(lst_column[0], false,
						false);
			}
		} else {
			colStructure = getColumnStructureObject(lst_column[0], false, false);
		}

		return colStructure;
	}

	/**
	 * getDataType :Parse Datatype by Splitting Datatype string contains round
	 * brackets
	 * 
	 * @param tmpDataType
	 *            :DB2 DDL Datatype String
	 * @return String : String contains datatype after parsing
	 */
	private String getDataType(String tmpDataType) {
		String type = "";
		if (tmpDataType.contains("(")) {
			String[] tmp = tmpDataType.split("\\(");
			type = tmp[0];
		} else {
			type = tmpDataType;
		}
		return type;
	}

	/**
	 * getPrecision :Parse decimal precision by Splitting string contains round
	 * brackets
	 * 
	 * @param tmpDataType
	 *            :DB2 DDL Datatype String
	 * @return String : String contains Precision value after parsing
	 */
	private Integer getPrecision(String tmpDataType) {
		Integer precision = 0;
		if (tmpDataType.contains("(")) {
			String[] tmp = tmpDataType.split("\\(");
			if (tmpDataType.contains(","))
				precision = Integer.parseInt(tmp[1].substring(0, tmp[1].indexOf(",")));
			else
				precision = Integer.parseInt(tmp[1].substring(0, tmp[1].length() - 1));
		} else {
			if (tmpDataType.equalsIgnoreCase("DECIMAL")) {
				precision = 10;
			} else {
				precision = 0;
			}
		}
		return precision;
	}

	/**
	 * getScale :Parse decimal scale by Splitting string contains round brackets
	 * 
	 * @param tmpDataType
	 *            :DB2 DDL Datatype String
	 * @return String : String contains scale value after parsing
	 */
	private Integer getScale(String tmpDataType) {
		Integer scale = 0;
		if (tmpDataType.contains("(")) {
			String[] tmp = tmpDataType.split("\\(");
			if (tmpDataType.contains(",")) {
				if (tmpDataType.contains(")"))
					scale = Integer.parseInt(tmp[1].substring(tmp[1].indexOf(",") + 1,tmp[1].length() - 1));
				else
					scale = Integer.parseInt(tmp[1].substring(tmp[1].indexOf(",") + 1,tmp[1].length()));
			} else {
				scale = 0;
			}
		} else {
			scale = 0;
		}
		return scale;
	}

	/**
	 * getScale :Parse and get length for datatype
	 * 
	 * @param tmpDataType
	 *            :DB2 DDL Datatype String
	 * @return String : String contains length value after parsing
	 */
	private Integer getLength(String tmpDataType) {
		Integer length = 0;
		if (tmpDataType.contains("(")) {
			String[] tmp = tmpDataType.split("\\(");
			if (tmpDataType.contains(")")) {
				if (tmpDataType.contains(","))
					length = Integer.parseInt(tmp[1].substring(0, tmp[1].indexOf(",")));
				else
					length = Integer.parseInt(tmp[1].substring(0, tmp[1].length() - 1));
			} else {
				length = Integer.parseInt(tmp[1]);
			}
		} else {
			length = 0;
		}
		return length;
	}

}
