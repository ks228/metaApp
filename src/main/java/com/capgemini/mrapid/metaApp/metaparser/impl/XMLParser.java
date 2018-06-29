package com.capgemini.mrapid.metaApp.metaparser.impl;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.XML;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.pojo.ColumnStructure;
import com.capgemini.mrapid.metaApp.pojo.XMLTableStructure;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;
import com.capgemini.mrapid.metaApp.utils.PKSupplementaryXML;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.google.gson.Gson;

/**
 * XML Parser Class
 * @author Anurag Udasi
 *
 */
public class XMLParser implements IParser {

	final static Logger log = Logger.getLogger(XMLParser.class);

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
			Map<String, String> excelPropertyMap, String classification) {

		date_column_position = new ArrayList<Integer>();
		time_column_position = new ArrayList<Integer>();
		length_column_position_and_value = new ArrayList<String>();
		srcSeqCount = 1;
		srcRowCount = 0;
		descSeqCount = 1;
		tableName = table;
		JSONObject xmlFinalJSONObject = new JSONObject();
		JSONObject xmlJSONObj;
		String xmlFileBuffer = new String();
		Gson gson = new Gson();
		try {
			for (String temp : schema) {
				xmlFileBuffer += temp + " ";
			}
			xmlJSONObj = XML.toJSONObject(xmlFileBuffer);
			// log.info("xmlFileBuffer" + xmlFileBuffer);
			ConfigUtils config = new ConfigUtils();

			XMLTableStructure tableStructure = gson.fromJson(xmlJSONObj.getString(Constants.XML_TABLE_TAG),XMLTableStructure.class);
			ArrayList<ColumnStructure> xmlColumnStructure = tableStructure.getSourceColumn();

			ArrayList<ColumnStructure> xmlSrcHeadeRrColumnStructure = new ArrayList<ColumnStructure>();// getSrcHeaderColumns(propertiesMap, metaInformation,classification);
			ArrayList<ColumnStructure> xmlDescHeaderColumnStructure = new ArrayList<ColumnStructure>();//getDescHeaderColumns(propertiesMap, metaInformation,classification);

			String supplementaryPKFilePath = propertiesMap.get(Constants.SUPPLEMENTARY_PK);
			if (supplementaryPKFilePath != null) {
				Workbook primaryKeyFile = config.readExcelFile(supplementaryPKFilePath);
				tablePKValues = PKSupplementaryXML.readSupplementaryPrimaryKeysDynamic(primaryKeyFile);
			}

//			int noOfRowsTOSkip = getIngnoredRowCount(propertiesMap);
//			log.info("Number of Rows to Skip :- " + noOfRowsTOSkip);

			JSONObject xmlSourceSchema = getSourceSchema(xmlColumnStructure,xmlSrcHeadeRrColumnStructure);
			JSONObject xmlDestinationSchema = getDestinationSchema(xmlColumnStructure, xmlDescHeaderColumnStructure, datatypeMap);
			JSONObject xmlTransformationsSchema = ParserUtils.getTransformationsSchema(propertiesMap);

			date_column_position = ParserUtils.getDateColumnPosition(xmlSourceSchema, datatypeMap);
			time_column_position = ParserUtils.getTimeColumnPosition(xmlSourceSchema, datatypeMap);
			length_column_position_and_value = ParserUtils.getLengthColumnPosition(xmlSourceSchema, datatypeMap);

			JSONObject modifiedMetaInformationJsonObject = ParserUtils.getModifiedMetaInformation(metaInformation, srcRowCount,date_column_position, time_column_position,length_column_position_and_value, propertiesMap,xmlSourceSchema);

			xmlFinalJSONObject.put(JSONConstants.SOURCE_SCHEMA, xmlSourceSchema);
			xmlFinalJSONObject.put(JSONConstants.DESTINATION_SCHEMA,xmlDestinationSchema);
			xmlFinalJSONObject.put(JSONConstants.TRANSFORMATION_SCHEMA,xmlTransformationsSchema);
			xmlFinalJSONObject.put(JSONConstants.METAINFORMATION_SCHEMA,modifiedMetaInformationJsonObject);
			// log.info("finalJSONObject"+finalJSONObject);

		} catch (JSONException e) {
			log.error(e.getMessage());
			return null;
		}
		return xmlFinalJSONObject;

	}

//	/**
//	 * @param propertiesMap
//	 * @return
//	 */
//	private int getIngnoredRowCount(Map<String, String> propertiesMap){
//		int noOfRows = 0;
//		String[] bussJournalDateTimeColumn = propertiesMap.get(Constants.BUSS_JOURNAL_DATE_TIME_COLS)
//				.split("\\,");
//		noOfRows = bussJournalDateTimeColumn.length;
//		return noOfRows;
//	}

//	/**
//	 * Generate Array list of header columns for source schema
//	 * 
//	 * @param propertiesMap
//	 * @param metaInformation
//	 * @param classification
//	 * @return
//	 */
//	private ArrayList<ColumnStructure> getSrcHeaderColumns(
//			Map<String, String> propertiesMap, JSONObject metaInformation, String classification) {
//		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();
//
//		// Add rowid in source schema (JSON)
//		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(
//				propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true, true);
//		headerStructure.add(uniqueIdStructure);
//
//		// Add CDC columns if srcType is Cdc
//		if (metaInformation.has("srctype")) {
//			try {
//				String systemType = metaInformation.getString("srctype");
//				if (systemType.equalsIgnoreCase("CDC")) {
//					String[] cdcColumn = propertiesMap
//							.get(Constants.CDC_COULMN).split("\\,");
//					for (int i = 0; i < cdcColumn.length; i++) {
//						ColumnStructure cdcColStructure = new ColumnStructure();
//						cdcColStructure = getHeaderColumnStructure(
//								cdcColumn[i], false, true);
//						headerStructure.add(cdcColStructure);
//					}
//				}
//			} catch (JSONException e) {
//				log.error(e.getMessage());
//			}
//		}
//		return headerStructure;
//	}

//	/**
//	 * 
//	 * @param propertiesMap
//	 * @param metaInformation
//	 * @param classification
//	 * @return
//	 */
//	private ArrayList<ColumnStructure> getDescHeaderColumns(
//			Map<String, String> propertiesMap, JSONObject metaInformation, String classification) {
//		ArrayList<ColumnStructure> headerStructure = new ArrayList<ColumnStructure>();
//		ColumnStructure uniqueIdStructure = getHeaderColumnStructure(
//				propertiesMap.get(Constants.UNIQUE_ID_COLUMN), true, false);
//
//		headerStructure.add(uniqueIdStructure);
//		if(!classification.equalsIgnoreCase(Constants.TABLE_TYPE_RECON)){
//			// Add BUSS_DATE_COLS JOURNAL_TIME_COLS		
//			String[] bussJournalDateTimeColumn = propertiesMap.get(Constants.BUSS_JOURNAL_DATE_TIME_COLS)
//					.split("\\,");
//			for (int i = 0; i < bussJournalDateTimeColumn.length; i++) {
//				ColumnStructure bussDateStructure = new ColumnStructure();
//				bussDateStructure = getHeaderColumnStructure(bussJournalDateTimeColumn[i],
//						false, false);
//				headerStructure.add(bussDateStructure);
//			}
//		}
//
//		// add CDC columns if srcType is CDC
//		String systemType = "";
//		if (metaInformation.has("srctype")) {
//			try {
//				systemType = metaInformation.getString("srctype");
//				if (systemType.equalsIgnoreCase("CDC")) {
//					String[] cdcColumn = propertiesMap
//							.get(Constants.CDC_COULMN).split("\\,");
//					for (int i = 0; i < cdcColumn.length; i++) {
//						ColumnStructure cdcColStructure = new ColumnStructure();
//						cdcColStructure = getHeaderColumnStructure(
//								cdcColumn[i], false, false);
//						headerStructure.add(cdcColStructure);
//					}
//				}
//			} catch (JSONException e) {
//				e.printStackTrace();
//			}
//		}
//		return headerStructure;
//	}

//	/**
//	 * Generate column structure for source header columns
//	 * 
//	 * @param columnName
//	 *            - column name
//	 * @param not_null
//	 *            - flag for not null check
//	 * @param is_source
//	 * @return
//	 */
//	private ColumnStructure getHeaderColumnStructure(String columnName,
//			Boolean not_null, Boolean is_source) {
//		ColumnStructure headerColumnStructure = new ColumnStructure();
//		headerColumnStructure.setName(columnName);
//		headerColumnStructure.setType("string");
//		headerColumnStructure.setLength(0);
//		headerColumnStructure.setPrecision(0);
//		headerColumnStructure.setScale(0);
//		if (not_null)
//			headerColumnStructure.setNullable(false);
//		else
//			headerColumnStructure.setNullable(true);
//
//		if (is_source) {
//			headerColumnStructure.setSeq(srcSeqCount);
//			srcRowCount++;
//			srcSeqCount++;
//		} else {
//			headerColumnStructure.setSeq(descSeqCount);
//			descSeqCount++;
//		}
//		return headerColumnStructure;
//	}

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

		ArrayList<Integer> src_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> src_primary_key = new ArrayList<String>();

		JSONObject jObject = new JSONObject();
		try {
			JSONArray jArray = new JSONArray();
//			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME,
						columnStructureList.getName());
				dblJSON.put(JSONConstants.COLUMN_TYPE,
						columnStructureList.getType());
				dblJSON.put(JSONConstants.COLUMN_LENGTH,
						columnStructureList.getLength());
				dblJSON.put(JSONConstants.COLUMN_PRECISION,
						columnStructureList.getPrecision());
				dblJSON.put(JSONConstants.COLUMN_SCALE,
						columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, srcSeqCount);
				if (!columnStructureList.getNullable()) {
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,
							JSONConstants.NOT_NULL_TRUE);
					if (columnStructureList.getKey()) {
						src_primary_key_position.add(srcSeqCount - 1);
						src_primary_key.add(columnStructureList.getName());
					}
				} else {
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,
							JSONConstants.NOT_NULL_FALSE);
				}
				srcSeqCount++;
				srcRowCount++;
				jArray.put(dblJSON);
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (tablePKValues != null) {
				if (tablePKValues.containsKey(tableName)) {
					jArrayPrimaryKey = PKSupplementaryXML
							.getSupplementaryPrimaryKeys(tableName,
									tablePKValues);
				} else {
					if (!src_primary_key.isEmpty()) {
						for (String primaryKey : src_primary_key) {
							jArrayPrimaryKey.put(primaryKey);
						}
					}
				}
			}

			JSONArray jKeyArray = new JSONArray();
			if (tablePKValues != null) {
				if (tablePKValues.containsKey(tableName)) {
					ArrayList<Integer> keyPositions = PKSupplementaryXML
							.getSupplementaryPrimaryKeyPosition(tableName,
									tablePKValues, jArray);
					if (keyPositions.size() >= 1)
						jKeyArray.put(StringUtils.join(keyPositions, ','));
					else {
						log.error("Primary key defined in Excel "
								+ jArrayPrimaryKey
								+ " not present in Schema of " + tableName);
					}
				} else {
					if (src_primary_key_position.size() > 0) {
						String keyPositions = StringUtils.join(
								src_primary_key_position, ',');
						jKeyArray.put(keyPositions);
					}
				}
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
	 * getDestinationSchema: extract Destination Schema,
	 * 
	 * @param ColumnStructure
	 *            : ArrayList contains list of ColumnStructure object
	 * @param HeaderColumnStructure
	 * @param datatypeMap
	 * @return JSONObject : DestinationSchema in json format
	 */
	private JSONObject getDestinationSchema(
			ArrayList<ColumnStructure> ColumnStructure,
			ArrayList<ColumnStructure> HeaderColumnStructure,
			Map<String, String> datatypeMap) {
		JSONObject jObject = new JSONObject();
		ArrayList<Integer> desc_primary_key_position = new ArrayList<Integer>();
		ArrayList<String> desc_primary_key = new ArrayList<String>();

		try {
			JSONArray jArray = new JSONArray();
//			jArray = ParserUtils.getHeaderJsonArray(HeaderColumnStructure);
			for (ColumnStructure columnStructureList : ColumnStructure) {
				JSONObject dblJSON = new JSONObject();
				dblJSON.put(JSONConstants.COLUMN_NAME,
						ParserUtils.cleanString(columnStructureList.getName()));
				String orcDataType= ParserUtils
						.convertDataTypeForHive(columnStructureList.getType(),
								datatypeMap,
								columnStructureList.getPrecision(),
								columnStructureList.getScale());

				dblJSON.put(JSONConstants.COLUMN_TYPE, orcDataType);
				dblJSON.put(JSONConstants.COLUMN_LENGTH, ParserUtils
						.convertLengthForHive(columnStructureList.getType(),
								columnStructureList.getLength(), datatypeMap));

				//Set precision value for decimal datatype as 10 if in xml precison is zero
				String dataTypeMapKey = orcDataType + JSONConstants.ORC_PRECISION_SEPERATOR + JSONConstants.ORC_PRECISION_SUFFIX;
				if(datatypeMap.get(dataTypeMapKey) != null && columnStructureList.getPrecision() == 0)
				{					
					dblJSON.put(JSONConstants.COLUMN_PRECISION,
							datatypeMap.get(dataTypeMapKey));
				}
				else
				{
					dblJSON.put(JSONConstants.COLUMN_PRECISION,
							columnStructureList.getPrecision());
				}	

				dblJSON.put(JSONConstants.COLUMN_SCALE,
						columnStructureList.getScale());
				dblJSON.put(JSONConstants.COLUMN_SEQUENCE, descSeqCount);

				if (!columnStructureList.getNullable()) {
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,
							JSONConstants.NOT_NULL_TRUE);
					if (columnStructureList.getKey()) {
						desc_primary_key_position.add(descSeqCount - 1);
						desc_primary_key.add(columnStructureList.getName());
					}
				} else {
					dblJSON.put(JSONConstants.COLUMN_NULLABLE,
							JSONConstants.NOT_NULL_FALSE);
				}
				jArray.put(dblJSON);
				descSeqCount++;
			}

			JSONArray jArrayPrimaryKey = new JSONArray();
			if (tablePKValues != null) {
				if (tablePKValues.containsKey(tableName)) {
					jArrayPrimaryKey = PKSupplementaryXML
							.getSupplementaryPrimaryKeys(tableName,
									tablePKValues);
				} else {
					if (!desc_primary_key.isEmpty()) {
						for (String primaryKey : desc_primary_key) {
							jArrayPrimaryKey.put(primaryKey);
						}
					}
				}
			}

			JSONArray jKeyArray = new JSONArray();
			if (tablePKValues != null) {
				if (tablePKValues.containsKey(tableName)) {
					ArrayList<Integer> keyPositions = PKSupplementaryXML
							.getSupplementaryPrimaryKeyPosition(tableName,
									tablePKValues, jArray);
					if (keyPositions.size() >= 1)
						jKeyArray.put(StringUtils.join(keyPositions, ','));
				} else {
					if (desc_primary_key_position.size() > 0) {
						String keyPositions = StringUtils.join(
								desc_primary_key_position, ',');
						jKeyArray.put(keyPositions);
					}
				}
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
