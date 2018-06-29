/**
 * MetaDataUtils - utility for all common operation for metaData
 * Prepare metadata for tables - <srcSystem>_all_countries, <srcSystem>_all_tables, dotopal_all_tab_cols
 * Read property file for metaData, get version , time information etc
 * @author Anuradha Dede
 *
 */
package com.capgemini.mrapid.metaApp.metadata.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.JSONConstants;
import com.capgemini.mrapid.metaApp.constants.MetaDataConstants;
import com.capgemini.mrapid.metaApp.constants.MetaInfoConstants;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.integration.impl.HiveHandle;
import com.capgemini.mrapid.metaApp.pojo.AllCountriesStructure;
import com.capgemini.mrapid.metaApp.pojo.AllTableColumnStructure;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;

public class MetaDataUtils {
	HDFSHandle hdfsHandler = new HDFSHandle();
	HiveHandle Hivehandler = new HiveHandle();
	final Logger log = Logger.getLogger(MetaDataUtils.class);
	StringWriter stack = new StringWriter();
	String delimiter = "";
	public String updTableName = null;

	/**
	 * getCountryMeta: construct all countries table hive query
	 * @param srcSystem: srcSystem value
	 * @param cntryName: cntryName value return
	 * @param latestversion
	 * @param defaultValueMap
	 * @param enableHiveTransaction
	 * @return
	 */
	public String getCountryMeta(String srcSystem, String cntryName,
			String latestversion, Map<String, String> defaultValueMap,String enableHiveTransaction ) {
		delimiter = getDelimiter(srcSystem, cntryName, defaultValueMap);
		AllCountriesStructure dataobj = new AllCountriesStructure();
		log.info(srcSystem + ":" + cntryName + ":"
				+ "constructing allcountries string with version"
				+ latestversion);
		String dataLine="";
		
		if(enableHiveTransaction.equalsIgnoreCase("true"))
		{
		 dataLine = "'"
		 + srcSystem
		 + "'"
		 + delimiter
		 + "'"
		 + cntryName
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap.get(MetaDataConstants.DATA_PATH)
		 + "'"
		 + delimiter
		 + "'"
		 + latestversion
		 + "'"
		 + delimiter
		 + "'"
		 + dataobj.getSource_group()
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap.get(MetaDataConstants.THRESHOLD_VAL)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap.get(MetaDataConstants.CUT_OFF_HOUR)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap
		 .get(MetaDataConstants.RETENTION_PERIOD_HDFSFILE)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap
		 .get(MetaDataConstants.RETENTION_PERIOD_EDGE_NODE)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap.get(MetaDataConstants.RETENTION_PERIOD_OPS)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap
		 .get(MetaDataConstants.RETENTION_PERIOD_BACKUP)
		 + "'"
		 + delimiter
		 + "'"
		 + defaultValueMap
		 .get(MetaDataConstants.RETENTION_PERIOD_EOD_DATE_FILE)
		 + "'" + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.TIME_ZONE_VALUE) + "'"
		 + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.MRAPID_SUBSCRIPTION)
		 + "'";
		}
		else{
			 dataLine = srcSystem.toUpperCase()
				+ delimiter
				+ cntryName.toUpperCase()
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.DATA_PATH)
				+ delimiter
				+ latestversion
				+ delimiter
				+ dataobj.getSource_group()
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.THRESHOLD_VAL)
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.CUT_OFF_HOUR)
				+ delimiter
				+ defaultValueMap
						.get(MetaDataConstants.RETENTION_PERIOD_HDFSFILE)
				+ delimiter
				+ defaultValueMap
						.get(MetaDataConstants.RETENTION_PERIOD_EDGE_NODE)
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.RETENTION_PERIOD_OPS)
				+ delimiter
				+ defaultValueMap
						.get(MetaDataConstants.RETENTION_PERIOD_BACKUP)
				+ delimiter
				+ defaultValueMap
						.get(MetaDataConstants.RETENTION_PERIOD_EOD_DATE_FILE)
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.TIME_ZONE_VALUE)
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.MRAPID_SUBSCRIPTION);
		}
		return dataLine;

	}

	/**
	 * getTableMeta: construct all tables table query
	 * @param srcSystem : srcSystem value
	 * @param cntryName : cntryName value return
	 * @param tableName
	 * @param classification
	 * @param thresholdVal
	 * @param latestversion
	 * @param server
	 * @param port
	 * @param defaultValueMap
	 * @param enableHiveTransaction
	 * @return
	 * @throws ParseException
	 */
	public String getTableMeta(String srcSystem, String cntryName,
			String tableName, String classification,String thresholdVal, String latestversion,
			String server, int port, Map<String, String> defaultValueMap,String enableHiveTransaction)
			throws ParseException {

		delimiter = getDelimiter(srcSystem, cntryName, defaultValueMap);
		log.info(srcSystem + ":" + cntryName + ":"
				+ "constructing alltables string for table" + tableName
				+ " and version" + latestversion);
         String dataLine="";
		
		if(enableHiveTransaction.equalsIgnoreCase("true"))
		{

		 dataLine = "'" + cntryName + "'" + delimiter + "'" + tableName
		 + "'" + delimiter + "'" + srcSystem + "'" + delimiter + "'"
		 + classification + "'" + delimiter + "'"
//		 + defaultValueMap.get(MetaDataConstants.THRESHOLD_VAL) + "'"
		 +thresholdVal + "'"
		 + delimiter + "'" + defaultValueMap.get(MetaDataConstants.USER)
		 + "'" + delimiter + "'" + server + "'" + delimiter + port
		 + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.OTHER_INFO) + "'"
		 + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.CURRENT_VERSION) + "'"
		 + delimiter + "'" + latestversion + "'" + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.VALID) + "'"
		 + delimiter + "'" + getCurrentTime() + "'" + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.VALID_TO) + "'"
		 + delimiter + "'" + getCurrentTime() + "'" + delimiter + "'"
		 + getCurrentTime() + "'" + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.NEW_LINE_CHAR) + "'"
		 + delimiter + "'"
		 + defaultValueMap.get(MetaDataConstants.MRAPID_SUBSCRIPTION)
		 + "'";
		}
		else{
		 dataLine = cntryName.toUpperCase() + delimiter
				+ tableName.toUpperCase() + delimiter + srcSystem.toUpperCase()
				+ delimiter + classification.toUpperCase() + delimiter
//				+ defaultValueMap.get(MetaDataConstants.THRESHOLD_VAL)
				+thresholdVal
				+ delimiter + defaultValueMap.get(MetaDataConstants.USER)
				+ delimiter + server + delimiter + port + delimiter
				+ defaultValueMap.get(MetaDataConstants.OTHER_INFO) + delimiter
				+ defaultValueMap.get(MetaDataConstants.CURRENT_VERSION)
				+ delimiter + latestversion + delimiter
				+ defaultValueMap.get(MetaDataConstants.VALID) + delimiter
				+ getCurrentTime() + delimiter
				+ defaultValueMap.get(MetaDataConstants.VALID_TO) + delimiter
				+ getCurrentTime() + delimiter + getCurrentTime() + "'"
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.NEW_LINE_CHAR)
				+ delimiter
				+ defaultValueMap.get(MetaDataConstants.MRAPID_SUBSCRIPTION);
		}
		 return dataLine;

	}

	/**
	 * getTableColumsMeta: contruct all table columns hive query
	 * @param jsonstr
	 * @param srcSystem : srcSystem value
	 * @param country : cntryName value return
	 * @throws IOException
	 * @param tableName
	 * @param latestversion
	 * @param defaultValueMap
	 * @param sourcePropertiesMap
	 * @param enableHiveTransaction
	 * @return
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public ArrayList<String> getTableColumsMeta(String jsonstr,
			String srcSystem, String country, String tableName,
			String latestversion, Map<String, String> defaultValueMap,
			Map<String, String> sourcePropertiesMap,String enableHiveTransaction)
			throws NumberFormatException, ParseException {
		int columnorder = 1;
		delimiter = getDelimiter(srcSystem, country, defaultValueMap);
		AllTableColumnStructure columnObj = new AllTableColumnStructure();
		String dataLine = "";
		ArrayList<String> columnArr = new ArrayList<String>();
		ArrayList<String> cdcColumnArr = getCDCColList(sourcePropertiesMap);
		ArrayList<String> bussJournalArr = getBussDateColList(sourcePropertiesMap);
		log.info(srcSystem + ":" + country + ":"
				+ "constructing alltabcolumns string for table" + tableName
				+ " and version" + latestversion);
		try {
			String nullable = "";
			String Primary_column_indicator = "N";
			JSONObject jsonobj = null;
			jsonstr = jsonstr.replaceAll("\n", "\\n");
			jsonobj = new JSONObject(jsonstr);
			JSONObject destSch = jsonobj.getJSONObject(JSONConstants.DESTINATION_SCHEMA);
			JSONArray columnsArray = destSch.getJSONArray(JSONConstants.COLUMNS_ARRAY);
			JSONArray primaryKeyArr = destSch
					.getJSONArray(JSONConstants.PRIMARY_KEY_POSITION);
			for (int i = 0; i < columnsArray.length(); i++) {

				JSONObject columnjson = new JSONObject();
				columnjson = columnsArray.getJSONObject(i);

				if (!(cdcColumnArr.contains(columnjson.getString(JSONConstants.COLUMN_NAME))
						|| bussJournalArr.contains(columnjson
								.getString(JSONConstants.COLUMN_NAME)))) {

					columnObj.setColumn_name(columnjson.getString(JSONConstants.COLUMN_NAME)
							.toUpperCase());
					columnObj.setData_type(columnjson.getString(JSONConstants.COLUMN_TYPE));
					columnObj.setData_precision(Integer.parseInt(columnjson
							.getString(JSONConstants.COLUMN_PRECISION)));
					columnObj.setData_length(Integer.parseInt(columnjson
							.getString(JSONConstants.COLUMN_LENGTH)));
					if (columnjson.getString(JSONConstants.COLUMN_NULLABLE).equals("Yes")) {
						nullable = "Y";
					} else {
						nullable = "N";
					}
					if (primaryKeyArr.length() != 0) {
						for (int j = 0; j < primaryKeyArr.length(); j++) {
							String[] priKey = primaryKeyArr.getString(j).split(
									",");
							for (int z = 0; z < priKey.length; z++) {
								if (Integer.parseInt(priKey[z]) == i) {
									Primary_column_indicator = "Y";
									break;
								} else {
									Primary_column_indicator = "N";
								}
							}
						}
					}			
					if(enableHiveTransaction.equalsIgnoreCase("true"))
					{
					 dataLine = "'"
					 + tableName
					 + "'"
					 + delimiter
					 + "'"
					 + columnObj.getColumn_name()
					 + "'"
					 + delimiter
					 + "'"
					 + country
					 + "'"
					 + delimiter
					 + "'"
					 + srcSystem
					 + "'"
					 + delimiter
					 + "'"
					 + columnjson.getString(JSONConstants.COLUMN_TYPE)
					 + "'"
					 + delimiter
					 + Integer.parseInt(columnjson.getString(JSONConstants.COLUMN_LENGTH))
					 + delimiter
					 + Integer.parseInt(columnjson.getString(JSONConstants.COLUMN_PRECISION))
					 + delimiter
					 + "'"
					 + Primary_column_indicator
					 + "'"
					 + delimiter
					 + "'"
					 + nullable
					 + "'"
					 + delimiter
					 + columnorder
					 + delimiter
					 + "'"
					 + defaultValueMap
					 .get(MetaDataConstants.CURRENT_VERSION) + "'"
					 + delimiter + "'" + latestversion + "'" + delimiter
					 + "'" + defaultValueMap.get(MetaDataConstants.VALID)
					 + "'" + delimiter + "'" + getCurrentTime() + "'"
					 + delimiter + "'"
					 + defaultValueMap.get(MetaDataConstants.VALID_TO) + "'"
					 + delimiter + "'" + getCurrentTime() + "'" + delimiter
					 + "'" + getCurrentTime() + "'" + delimiter + "'"
					 + columnObj.getComment() + "'";
					}else{
					dataLine = tableName.toUpperCase()

							+ delimiter

							+ columnObj.getColumn_name().toUpperCase()

							+ delimiter

							+ country.toUpperCase()

							+ delimiter

							+ srcSystem.toUpperCase()

							+ delimiter

							+ columnjson.getString(JSONConstants.COLUMN_TYPE).toUpperCase()

							+ delimiter
							+ Integer.parseInt(columnjson.getString(JSONConstants.COLUMN_LENGTH))
							+ delimiter
							+ Integer.parseInt(columnjson
									.getString(JSONConstants.COLUMN_PRECISION))
							+ delimiter

							+ Primary_column_indicator

							+ delimiter

							+ nullable

							+ delimiter
							+ columnorder
							+ delimiter

							+ defaultValueMap
									.get(MetaDataConstants.CURRENT_VERSION)
							+ delimiter + latestversion + delimiter
							+ defaultValueMap.get(MetaDataConstants.VALID)
							+ delimiter + getCurrentTime() + delimiter
							+ defaultValueMap.get(MetaDataConstants.VALID_TO)
							+ delimiter + getCurrentTime() + delimiter
							+ getCurrentTime() + delimiter
							+ columnObj.getComment();
					}
					columnArr.add(dataLine);
					columnorder++;
				}

			}

		} catch (JSONException e) {
			e.printStackTrace(new PrintWriter(stack));
			log.error("Hive Connecection Error" + stack.toString());
		} catch (NumberFormatException e) {
			System.err
					.println("Problem in getTableColumsMeta(). Cannot convert from one form to another");
			e.printStackTrace();
		}

		return columnArr;
	}

	/**
	 * getTableName: reading table from tablelist for srcSystem and country
	 * @param srcSystem
	 * @param cntryName
	 * @param configPath
	 * @return
	 */
	public String getTableName(String srcSystem, String cntryName,
			String configPath) {
		ConfigUtils config = new ConfigUtils();
		Properties property = null;
		String tableName = null;
		// get table name
		try {
			property = config.getConfigValues(configPath);
			Map<String, TableListStructure> tableListMap = config
					.readTableList(property.getProperty(srcSystem
							+ Constants.TABLE_LIST_PATH));
			log.info(srcSystem
					+ ":"
					+ cntryName
					+ ":"
					+ "reading table list from"
					+ property
							.getProperty(srcSystem + Constants.TABLE_LIST_PATH)
					+ "file");
			for (Map.Entry<String, TableListStructure> table : tableListMap
					.entrySet()) {
				tableName = table.getValue().getName();

			}
		} catch (Exception e) {
			e.printStackTrace(new PrintWriter(stack));
			log.error(srcSystem
					+ ":"
					+ cntryName
					+ ":"
					+ " failed to read table list "
					+ property
							.getProperty(srcSystem + Constants.TABLE_LIST_PATH)
					+ stack.toString());
		}
		return tableName;

	}
	/**
	 * getVersion: get version from all_tab_columns table
	 * @param srcSystem
	 * @param country
	 * @param hadoopProperties
	 * @param flag
	 * @param hdfsURL
	 * @param metadataDBNAME
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String getVersion(String srcSystem, String country,
			Map<String, String> hadoopProperties, String flag, String hdfsURL,
			String metadataDBNAME)

	throws IOException, SQLException, ClassNotFoundException {
		log.info(srcSystem + ":" + country + ":"
				+ "Getting version from metadata_all_tab_colums table");
		String latestversion = "0";
		List<String> parameters = new ArrayList<String>();
		String statement = "max(version)";
		parameters.add("source");
		parameters.add("country_name");
		String query =constructSelectQuery(metadataDBNAME, srcSystem, flag,
				parameters, statement,null);
		latestversion = Hivehandler.selectMaxVersion(hadoopProperties, query,
				country.toUpperCase(), srcSystem.toUpperCase(), hdfsURL);
		if (!(latestversion.equalsIgnoreCase("0"))) {
			log.info(srcSystem + ":" + country + ":" + "inside getVersion"
					+ latestversion);
			if (flag.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_COUNTRY)) {
				latestversion = "-1";
			}
		}

		log.info(srcSystem + ":" + country + ":"
				+ "last version of metadata_all_tab_colums" + latestversion);
		return latestversion;

	}

	/**
	 * get metadata table's delimiter
	 * @param srcSystem
	 * @param country
	 * @param defaultValueMap
	 * @return
	 */
	public String getDelimiter(String srcSystem, String country,
			Map<String, String> defaultValueMap) {
		if (defaultValueMap.get(Constants.METADATA_DEL).contains("u")) {

			delimiter = StringEscapeUtils.unescapeJava(defaultValueMap
					.get(Constants.METADATA_DEL));

		} else {

			delimiter = defaultValueMap.get(Constants.METADATA_DEL);
		}

		return delimiter;
	}


	/**
	 * @param changeTableList
	 * @return
	 */
	public Map<String, Map<String, String>> getSchemaTablistMap(
			String changeTableList) {
		Map<String, Map<String, String>> schemaTablistMap = new HashMap<String, Map<String, String>>();
		String[] evolvedRecord = changeTableList.split("\\n");
		for (int i = 0; i < evolvedRecord.length; i++) {
			Map<String, String> tablelistMap = new HashMap<String, String>();
			String str = evolvedRecord[i];
			String[] tableRecord = str.split(Pattern.quote("|"));
			if (tableRecord.length >= 2) {
				tablelistMap.put(Constants.TYPE, tableRecord[1]);
				tablelistMap.put(Constants.VERSION, tableRecord[2]);
				schemaTablistMap.put(tableRecord[0], tablelistMap);
			}

		}

		log.info("schemaTablistMap" + schemaTablistMap.toString());
		return schemaTablistMap;
	}

	/**
	 * readFile: read File file
	 * @param fileName: File To be read
	 * @throws IOException
	 * 
	 */
	@SuppressWarnings("unused")
	public String readFile(String fileName) throws IOException {
		BufferedReader br = null;
		String filestr = "";
		String line = "";
		try {
			br = new BufferedReader(new FileReader(fileName));
			StringBuilder sb = new StringBuilder();
			if (br != null) {
				line = br.readLine();
			} else {
				log.error("Failed to read File" + fileName);
			}

			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			filestr = sb.toString();
		} finally {
			// try {
			br.close();
			// }
		}
		return filestr;
	}

	/**
	 * getCurrentTime: Get Current time format specified
	 * @throws ParseException
	 * 
	 */
	public String getCurrentTime() throws ParseException {
		DateFormat dateFormat = new SimpleDateFormat(
				MetaDataConstants.METADATA_DATA_FORMATE);
		String date = dateFormat.format(new Date());
		return date;
	}
	
	/**
	 * getThresholdVal: Get threshold value from metainfo
	 * 
	 * @throws ParseException
	 * @throws JSONException 
	 * 
	 */
	public String getThresholdVal(String jsonStr) throws ParseException, JSONException {
		JSONObject jsonobj = null;
		String thresholdVal;
		jsonStr = jsonStr.replaceAll("\n", "\\n");
		jsonobj = new JSONObject(jsonStr);
		JSONObject metaInfo = jsonobj.getJSONObject(JSONConstants.METAINFORMATION_SCHEMA);
		if(metaInfo.has(MetaInfoConstants.THRESHOLD_LIMIT)){
			thresholdVal = metaInfo.getString(MetaInfoConstants.THRESHOLD_LIMIT);
		}
		else{
			thresholdVal = "NULL";
		}
		return thresholdVal;
	}


	/**
	 * constructInsertQuery: construct insert query for metadata tables
	 * @param dbName
	 * @param source
	 * @param flag
	 * @param recordToInsert
	 * @param country
	 * @param defaultValueMap
	 * @param hadoopProperties
	 * @param hdfsURL
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public String constructInsertQuery(String dbName, String source,
			String flag, String recordToInsert, String country,
			Map<String, String> defaultValueMap,
			Map<String, String> hadoopProperties, String hdfsURL)
			throws IOException, SQLException {
		String hivetableName = "";
		String insertVal = "";
		delimiter = getDelimiter(source, country, defaultValueMap);
		if (flag.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_COUNTRY)) {
			hivetableName = source.toLowerCase()
					+ Constants.METADATA_ALL_COUNTRY;
			log.info(source + ":" + country + ":"
					+ "inserting allCountry record to table" + hivetableName);
			recordToInsert = recordToInsert.replaceAll(delimiter, ",");
			insertVal = recordToInsert;

		} else if (flag.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_TABLES)) {
			hivetableName = source.toLowerCase()
					+ Constants.METADATA_ALL_TABLES;
			log.info(source + ":" + country + ":"
					+ "inserting alltables record to table" + hivetableName);
			recordToInsert = recordToInsert.replaceAll(delimiter, ",");
			recordToInsert = recordToInsert.replaceAll("\n", "),(");
			insertVal = recordToInsert;
		} else if (flag
				.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_COLUMNS)) {
			hivetableName = source.toLowerCase()
					+ Constants.METADATA_ALL_COLUMNS;
			log.info(source + ":" + country + ":"
					+ "inserting allcolumns record to table" + hivetableName);
			recordToInsert = recordToInsert.replaceAll(delimiter, ",");
			recordToInsert = recordToInsert.replaceAll("\n", "),(");

			insertVal = recordToInsert;
		}

		String query1 = "INSERT INTO TABLE " + dbName + "." + hivetableName
				+ " values (" + insertVal + ")";
		try {

			Hivehandler.insert(query1, hadoopProperties, hdfsURL, source,
					country);
		} catch (SQLException e) {
			System.err.println("cannot perform insert query: " + e);
		}
		return query1;
	}
	
	/**
	 * constructSelectQuery: construct Select Query with conditional parameters
	 * @param dbName: Database Name
	 * @param source: srcSystem
	 * @param tableName: tableName
	 * @param parameters:conditional parameters list
	 * @param statement
	 * @param groupByStatement
	 * @throws ParseException
	 * 
	 */
	public String constructSelectQuery(String dbName, String source,
			String tableName, List<String> parameters, String statement,String groupByStatement) {
		String query1;
		if (tableName.equalsIgnoreCase(Constants.METADATA_ALL_COUNTRY)) {
			tableName = source.toLowerCase() + Constants.METADATA_ALL_COUNTRY;
		} else {
			tableName = source.toLowerCase() + Constants.METADATA_ALL_COLUMNS;
		}
		String parameterstr = "where ";
		for (int i = 0; i < parameters.size(); i++) {
			if (i < parameters.size() - 1) {
				parameterstr = parameterstr + parameters.get(i) + " = ? and ";
			} else {
				parameterstr = parameterstr + parameters.get(i) + " = ?";
			}
		}
		if(groupByStatement != null ){
			query1 = "select " + statement + " from " + dbName + "."
					+ tableName + " " + parameterstr + " " + groupByStatement;
		}else{
			query1 = "select " + statement + " from " + dbName + "."
					+ tableName + " " + parameterstr;

		}
		return query1;
	}

	/**
	 * getCDCColList: get CDC column list
	 *
	 * @param sourcePropertiesMap
	 *            : srcSystem sourcePropertiesMap
	 */
	public ArrayList<String> getCDCColList(
			Map<String, String> sourcePropertiesMap) {

		ArrayList<String> cdcArr = new ArrayList<String>();
		if(sourcePropertiesMap.containsKey(Constants.CDC_COULMN)){
			String CDC_COLS = sourcePropertiesMap.get(Constants.CDC_COULMN);
			String cdcColArr[] = CDC_COLS.split("\\,");
			if(cdcColArr.length > 0){
				for (int i = 0; i < cdcColArr.length; i++) {
					cdcArr.add(cdcColArr[i]);
				}
			}
		}
		return cdcArr;
	}

	/**
	 * getBussDateColList: get BussDateCol list
	 *
	 * @param sourcePropertiesMap
	 *            : srcSystem sourcePropertiesMap
	 * 
	 * 
	 */
	public ArrayList<String> getBussDateColList(
			Map<String, String> sourcePropertiesMap) {

		ArrayList<String> bussJournalArr = new ArrayList<String>();
		if(sourcePropertiesMap.containsKey(Constants.BUSS_JOURNAL_DATE_TIME_COLS)){
		 String  bussJournal_col=sourcePropertiesMap.get(Constants.BUSS_JOURNAL_DATE_TIME_COLS);
		 String[] bussJournalDateTimeCol = bussJournal_col.split(",");
		 if(bussJournalDateTimeCol.length >0){
			 for(int i=0; i < bussJournalDateTimeCol.length;i++){
				 bussJournalArr.add(bussJournalDateTimeCol[i]);
		 		}
		 	}
		}
		return bussJournalArr;
	}

	/**
	 * getJsonTableName: get tablename from metainfo
	 * @param jsonStr
	 * @param string tablename
	 * @throws JSONException
	 * 
	 * 
	 */
	public String getJsonTableName(String jsonStr) throws JSONException {

		JSONObject jsonobj = null;
		// System.out.println("jsonStr"+jsonStr);
		jsonStr = jsonStr.replaceAll("\n", "\\n");
		jsonobj = new JSONObject(jsonStr);
		JSONObject metaInfo = jsonobj.getJSONObject("MetaInformation");
		// System.out.println("metaInfo"+metaInfo);
		String tableName = metaInfo.getString("tablename");
		System.out.println("tablename" + tableName);
		return tableName;
	}

	/**
	 * checkHiveConnection: Check connection to hive
	 * @param srcSystem: srcSystem value
	 * @param country: cntryName value
	 * @param hadoopProperties : hadoop properties file data
	 * @param flag: table selection flag
	 * @param hdfsURL
	 * @param metadataDBNAME
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws: NumberFormatException
	 * 
	 */
	public boolean checkHiveConnection(String srcSystem, String country,
			Map<String, String> hadoopProperties, String flag, String hdfsURL,
			String metadataDBNAME) throws NumberFormatException,
			ClassNotFoundException, SQLException, IOException {
		boolean connFlag = false;
		if (!getVersion(srcSystem, country, hadoopProperties,
				MetaDataConstants.METADATA_ALL_COLUMNS, hdfsURL, metadataDBNAME)
				.equalsIgnoreCase(MetaDataConstants.CONNECTION_ERROR)) {
			connFlag = true;
		}

		return connFlag;
	}

	/**
	 * Construct metadata hive table update query
	 * @param dbName
	 * @param source
	 * @param flag
	 * @param country
	 * @param HiveURL
	 * @param coulmnToUpdate
	 * @param value
	 * @param parameters
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public String constructUpdateQuery(String dbName, String source,
			String flag, String country, String HiveURL, String coulmnToUpdate,
			String value, List<String> parameters) throws IOException,
			SQLException {
		if (flag.equalsIgnoreCase(Constants.METADATA_ALL_COUNTRY)) {
			log.info("country insert");
			// hivetableName = source.toLowerCase() +
			// Constants.METADATA_ALL_COUNTRY;
			updTableName = source.toLowerCase()
					+ Constants.METADATA_ALL_COUNTRY;
		} else if (flag.equalsIgnoreCase(Constants.METADATA_ALL_TABLES)) {
			log.info("table insert");
			updTableName = source.toLowerCase() + Constants.METADATA_ALL_TABLES;
		}
		String parameterstr = "where ";
		for (int i = 0; i < parameters.size(); i++) {
			if (i < parameters.size() - 1) {
				parameterstr = parameterstr + parameters.get(i) + " = ? and ";
			} else {
				parameterstr = parameterstr + parameters.get(i) + " = ?";
			}
		}
		String query1 = "update " + dbName + "." + updTableName + " set "
				+ coulmnToUpdate + " = " + "'" + value + "'" + " "
				+ parameterstr;

		try {
			Hivehandler.updateParam(HiveURL, query1, country, source, null);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return query1;
	}
	
	
}
