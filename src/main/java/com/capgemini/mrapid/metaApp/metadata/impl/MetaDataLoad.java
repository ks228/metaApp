/**
 * @author Anuradha Dede
 * MetaDataLoad loads metadata tables
 * 
 */
package com.capgemini.mrapid.metaApp.metadata.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.json.JSONException;
//import com.capgemini.mrapid.auditlogs.integration.impl.AuditLoghandler;
//import com.capgemini.mrapid.auditlogs.pojo.AuditLogPojo;
//import com.capgemini.mrapid.metaApp.constants.AuditConstants;
import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.MetaDataConstants;
import com.capgemini.mrapid.metaApp.exceptions.ConfigParametersReadException;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.metadata.IMetaDataLoad;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;
import com.google.common.base.Joiner;

public class MetaDataLoad implements IMetaDataLoad {

	final Logger log = Logger.getLogger(MetaDataLoad.class);
	public static Map<String, String> propertiesMap = new HashMap<String, String>();
	public Map<String, String> hadoopProperties = new HashMap<String, String>();
	public Map<String, String> deploymentProperties = new HashMap<String, String>();
	public Map<String, String> basicInfo = new HashMap<String, String>();
	public MetaDataUtils metaload = new MetaDataUtils();
	public Map<String, String> metaDataDefaultMap = new HashMap<String, String>();
	public ArrayList<String> checkNullStr = new ArrayList<String>();
	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	public Integer version = 1;
	public Integer allcountryversion = 0;
	public ConfigUtils config = new ConfigUtils();
	public String checkHive = "";
	public ArrayList<String> metaDataErrorTablist = new ArrayList<String>();
	public ArrayList<String> metaDataSucessTablist = new ArrayList<String>();
	public String server = "";
	public int port = 0;
	public Map<String, String> sourcePropertiesMap = null;
	public Map<String, String> countryPropertiesMap = null;
	public Map<String, Map<String, String>> schemaTablistMap = new HashMap<String, Map<String, String>>();

	public boolean DataLoad(String srcSystem, String country, String configPath) {

		boolean ConfigValue = true;
		try {
			initialization(srcSystem, country, configPath);
			if (ConfigValue) {

				intialDeployment(srcSystem, country);
				log.info(srcSystem + ":" + country + ":continousDeployment"
						+ "metadata insert  "
						+ metaDataSucessTablist.size()
						+ "table record out of " + schemaTablistMap.size());
				log.info("metaDataSucessTablist"
						+ metaDataSucessTablist.toString());
				log.info("schemaTablistMap" + schemaTablistMap.toString());
				if (metaDataErrorTablist.size() != 0) {
					log.info(srcSystem + ":" + country
							+ ":continousDeployment"
							+ "metadata failed tables  are "
							+ metaDataErrorTablist.size()
							+ "table record out of "
							+ schemaTablistMap.size()
							+ "Check failed table details in Audit logs");
					log.info("metaDataErrorTablist"
							+ metaDataErrorTablist.toString());
					log.info("schemaTablistMap"
							+ schemaTablistMap.toString());

				}
			} else {
				log.info("Configuration error please check above log will show missing config");
			}
		} catch (IOException e) {
			log.info("error" + e.getMessage());
		}

		return true;
	}


	/**
	 * @param srcSystem
	 * @param country
	 * @param configPath
	 */
	private void initialization(String srcSystem, String country, String configPath) {

		Properties property = config.getConfigValues(configPath);
		String[] hdfsserver = null;
		Date date = new Date(timestamp.getTime());
		hadoopProperties = config.readPropertyFile(srcSystem, country,
				property.getProperty(Constants.HADOOP_CONFIGURATION_PATH));
		sourcePropertiesMap = config.readPropertyFile(
				srcSystem,
				country,
				property.getProperty(srcSystem.toUpperCase()
						+ Constants.PROPERTIES_PATH));
		log.info(property.getProperty(srcSystem.toUpperCase() + "_"
				+ country.toUpperCase() + Constants.DEPLOYMENT_PROPERTIES_PATH));
		deploymentProperties = config.readPropertyFile(
				srcSystem,
				country,
				property.getProperty(srcSystem.toUpperCase() + "_"
						+ country.toUpperCase()
						+ Constants.DEPLOYMENT_PROPERTIES_PATH));
		countryPropertiesMap = config.readPropertyFile(
				srcSystem,
				country,
				property.getProperty(srcSystem.toUpperCase() + "_"
						+ country.toUpperCase()
						+ Constants.COUNTRY_PROPERTY_PATH));
		metaDataDefaultMap = config.readPropertyFile(
				srcSystem,
				country,
				property.getProperty(srcSystem.toUpperCase()
						+ Constants.METADATA_PROPERTIES_PATH));

		
		hdfsserver = hadoopProperties.get(Constants.HDFS_URL).split(":");
		server = hdfsserver[0];
		port = Integer.parseInt(hdfsserver[1]);
		try {
			//for demo
			if (metaload.checkHiveConnection(srcSystem.toUpperCase(),
					country.toUpperCase(), hadoopProperties,
					MetaDataConstants.METADATA_ALL_COLUMNS, hadoopProperties.get(Constants.HDFS_URL),
					deploymentProperties.get(Constants.METADATA_DBNAME))) {
				
				checkHive = metaload.getVersion(srcSystem.toUpperCase(),
						country.toUpperCase(), hadoopProperties,
						MetaDataConstants.METADATA_ALL_COLUMNS,
						hadoopProperties.get(Constants.HDFS_URL), deploymentProperties.get(Constants.METADATA_DBNAME));
				
				version = (Integer.parseInt(checkHive) + 1);
				
				allcountryversion = (Integer.parseInt(metaload.getVersion(
						srcSystem.toUpperCase(), country.toUpperCase(),
						hadoopProperties,
						MetaDataConstants.METADATA_ALL_COUNTRY,
						hadoopProperties.get(Constants.HDFS_URL), deploymentProperties.get(Constants.METADATA_DBNAME))) + 1);

				log.info("allcountryversion" + allcountryversion + "version"
						+ version);
			} else {
				log.error(srcSystem
						+ ":"
						+ country
						+ ":"
						+ "Unable to process further problem in hive connection  ");
				throw new ConfigParametersReadException(srcSystem, country);
			}
		} catch (NumberFormatException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further problem in hive connection throws "
					+ e.getMessage());
		} 
		catch (ClassNotFoundException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further problem in hive connection throws "
					+ e.getMessage());
		} catch (SQLException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further problem in hive connection throws "
					+ e.getMessage());
		} catch (IOException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further problem in hive connection throws "
					+ e.getMessage());
		}
		catch (ConfigParametersReadException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further problem in hive connection throws "
					+ e.getMessage());
		}

	}

	
	/**
	 * @param srcSystem
	 * @param country
	 * @throws IOException
	 */
	private void intialDeployment(String srcSystem, String country)
			throws IOException {

		String tablelistStr = "";
		String tableName = "";
		String str = "";
		String classification = "";
		HDFSHandle obj = new HDFSHandle();
		String finalcolumn = "";
		ArrayList<String> all_tablesArr = new ArrayList<String>();
		ArrayList<String> all_tab_coulmns = new ArrayList<String>();
		ArrayList<String> columnArr = new ArrayList<String>();
		String hdfspath = "";

		try {

			try {
				tablelistStr = obj.hdfsRead(
						deploymentProperties
						.get(Constants.SCHEMA_EVAL_LOG_FILE_PATH)
						+ "/"
						+ srcSystem
						+ "_"
						+ country
						+ Constants.TABLELIST, hadoopProperties
						.get(Constants.HDFS_URL));
			} catch (IOException e) {
				log.error(srcSystem + ":" + country + ":" + " table :"
						+ tableName
						+ "metaData failed to insert record not found"
						+ hdfspath + "not found");
				metaDataErrorTablist.add(tableName);

			}

			schemaTablistMap = metaload.getSchemaTablistMap(tablelistStr);
			// if (CommonUtils.containsNullKeysOrValues(schemaTablistMap)) {
			for (Map.Entry<String, Map<String, String>> schemaTables : schemaTablistMap
					.entrySet()) {
				log.info(srcSystem + ":" + country + ":"
						+ "constructing metadata record for table"
						+ schemaTables.getKey());

				tableName = schemaTables.getKey();
				propertiesMap = config.mapProperty(countryPropertiesMap,
						sourcePropertiesMap, hadoopProperties, tableName,
						srcSystem, country,"");
				Map<String, String> tableValues = schemaTables.getValue();
				version = Integer.parseInt(tableValues.get(Constants.VERSION));
				classification = tableValues.get(Constants.TYPE);
				hdfspath = (propertiesMap.get(Constants.JSON_OUTPUT_DIR)
						+ "/" + srcSystem + "_" + country + "_" + tableName + Constants.JSON_FORMAT)
						.toLowerCase();

				try {
					str = obj.hdfsRead(hdfspath.toLowerCase(),
							propertiesMap.get(Constants.HDFS_URL));
				} catch (IOException e) {
					log.error(srcSystem + ":" + country + ":" + " table :"
							+ tableName
							+ "metaData failed to insert record not found"
							+ hdfspath + "not found");
					metaDataErrorTablist.add(tableName);

				}

				checkNullStr.add(str);
				// all tables
				if (!(checkHive.equalsIgnoreCase("connection error") || str
						.isEmpty())) {
					//***************************************** Removed this call as threshold is NULL
					//						String thresholdVal= metaload.getThresholdVal(str);
					//********************************************************************************						
					if (tableName.equalsIgnoreCase(metaload
							.getJsonTableName(str))) {
						String all_tables = metaload.getTableMeta(srcSystem,
								country, tableName, classification,"NULL",
								version.toString(), server, port,
								metaDataDefaultMap,deploymentProperties.get(MetaDataConstants.ENABLEHIVETRANSACTION));
						all_tablesArr.add(all_tables);
						// all tab columns
						if (!(str.isEmpty())) {
							columnArr = metaload.getTableColumsMeta(str,
									srcSystem, country, tableName,
									version.toString(), metaDataDefaultMap,
									sourcePropertiesMap,deploymentProperties.get(MetaDataConstants.ENABLEHIVETRANSACTION));
							finalcolumn = Joiner.on("\n").join(columnArr);
							all_tab_coulmns.add(finalcolumn.toString());
						} else {
							log.error(srcSystem
									+ ":"
									+ country
									+ ":"
									+ " json  file for table "
									+ tableName
									+ "file"
									+ hdfspath
									+ "not found metaData failed to insert record not found");
							metaDataErrorTablist.add(tableName);
						}
						metaDataSucessTablist.add(metaload
								.getJsonTableName(str));
					} else {
						log.error(srcSystem
								+ ":"
								+ country
								+ ":"
								+ " table :"
								+ tableName
								+ "metaData failed to insert record not found"
								+ hdfspath + "not found");
						metaDataErrorTablist.add(tableName);
					}

				} else {
					log.error(srcSystem
							+ ":"
							+ country
							+ ":"
							+ "Unable to process further Check hive connection and json file for table "
							+ tableName + "file" + hdfspath + "not found");
					throw new ConfigParametersReadException(srcSystem,
							country);
				}

			}
			if (!(checkHive.equalsIgnoreCase("connection error"))) {
				String finalalltables = Joiner.on("\n").join(all_tablesArr);
				String finalalltabcoulmns = Joiner.on("\n").join(
						all_tab_coulmns);
				// WriteAllColumns
				WriteToHdfs(srcSystem, finalalltabcoulmns, country,
						version.toString(),
						MetaDataConstants.METADATA_ALL_COLUMNS,
						deploymentProperties.get(Constants.COMMON_BASE_PATH)
						, deploymentProperties.get(Constants.METADATA_DBNAME));
				// WriteAllTables
				WriteToHdfs(srcSystem, finalalltables, country,
						version.toString(),
						MetaDataConstants.METADATA_ALL_TABLES,
						deploymentProperties.get(Constants.COMMON_BASE_PATH)
						, deploymentProperties.get(Constants.METADATA_DBNAME));
				// WriteAllCntry
				if (allcountryversion == 1) {

					String allcountryString = metaload.getCountryMeta(
							srcSystem, country,
							allcountryversion.toString(),
							metaDataDefaultMap,deploymentProperties.get(MetaDataConstants.ENABLEHIVETRANSACTION));

					WriteToHdfs(srcSystem, allcountryString, country,
							version.toString(),
							MetaDataConstants.METADATA_ALL_COUNTRY,
							deploymentProperties.get(Constants.COMMON_BASE_PATH)
							, deploymentProperties.get(Constants.METADATA_DBNAME));

				}
			} else {
				log.error(srcSystem
						+ ":"
						+ country
						+ ":"
						+ "Unable to process further Check hive connection and json file for table "
						+ tableName + "file" + hdfspath + "not found");
				throw new ConfigParametersReadException(srcSystem, country);
			}

		} catch (ParseException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process throws ParseException further Check hive connection and json file for table "
					+ tableName + "file" + hdfspath + "not found");
		} catch (ConfigParametersReadException e) {
			log.error(e.get_srcSystem() + ":" + e.get_Country() + ":"
					+ "Unable to process further. Check "
					+ "configuration parameters in config files. Check hive "
					+ "connection and json file for table and country");

		} catch (JSONException e) {
			log.error(srcSystem
					+ ":"
					+ country
					+ ":"
					+ "Unable to process further Check hive connection and json file for table "
					+ tableName + "file" + hdfspath + "not found");
		}

	}

	/**
	 * @param srcSystem
	 * @param metaDataString
	 * @param country
	 * @param version
	 * @param metaDataFlag
	 * @param metadataHDFSPath
	 * @param metadataDBNAME
	 * @throws IOException
	 */
	private void WriteToHdfs(String srcSystem, String metaDataString,
			String country, String version, String metaDataFlag,
			String metadataHDFSPath, String metadataDBNAME) throws IOException {
		HDFSHandle hdfsHandler = new HDFSHandle();
		String hdfspath = "";
		String metaDataFileName = "";
		String problem = null;
		int index = 0;
		String key = null;
		boolean result = false;
		Map<String, String> detailedInfo = new HashMap<String, String>();
		int checkNullStrIteratorFlag = 0;

		try {
			if (metaDataFlag
					.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_COUNTRY)) {
				
				log.info(srcSystem + ":" + country + ":"
						+ "writing allCountry record to hdfs in file"
						+ hdfspath.toLowerCase() + "/"
						+ metaDataFileName.toLowerCase());

				metaDataFileName = country
						+ MetaDataConstants.METADATA_ALL_COUNTRY + "_V"
						+ version;

				hdfspath = metadataHDFSPath + "/" + metadataDBNAME + "/"
						+ srcSystem.toLowerCase()
						+ MetaDataConstants.METADATA_ALL_COUNTRY;

				log.info(srcSystem + ":" + country + ":"
						+ "writing allCountry record to hdfs in file"
						+ hdfspath + "/" + metaDataFileName);
			} else if (metaDataFlag
					.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_TABLES)) {
				
				metaDataFileName = country
						+ MetaDataConstants.METADATA_ALL_TABLES + "_V"
						+ version.toString();
				hdfspath = metadataHDFSPath + "/" + metadataDBNAME + "/"
						+ srcSystem.toLowerCase()
						+ MetaDataConstants.METADATA_ALL_TABLES;


				while (hdfsHandler.hdfsFileExits(hdfspath.toLowerCase() + "/"
						+ metaDataFileName, hadoopProperties.get(Constants.HDFS_URL))) {
					version = Integer.toString((Integer.parseInt(version) + 1));

					metaDataFileName = country
							+ MetaDataConstants.METADATA_ALL_TABLES + "_V"
							+ version;
					hdfspath = metadataHDFSPath + "/" + metadataDBNAME + "/"
							+ srcSystem.toLowerCase()
							+ MetaDataConstants.METADATA_ALL_TABLES;
				} 

				log.info(srcSystem + ":" + country + ":"
						+ "writing AllTables record to hdfs in file" + hdfspath
						+ "/" + metaDataFileName);
			} else if (metaDataFlag
					.equalsIgnoreCase(MetaDataConstants.METADATA_ALL_COLUMNS)) {
				log.info("inside metaData columns");
				
				Iterator checkNullStrIterator = checkNullStr.iterator();
				while (checkNullStrIterator.hasNext()) {
					if (checkNullStrIterator.next() == null) {
						problem = checkNullStr.get(index);
						checkNullStrIteratorFlag = 0;

						break;
					} else
						checkNullStrIteratorFlag = 1;
					index++;
				}
				

				metaDataFileName = country
						+ MetaDataConstants.METADATA_ALL_COLUMNS + "_V"
						+ version;
				hdfspath = metadataHDFSPath + "/" + metadataDBNAME + "/"
						+ srcSystem.toLowerCase()
						+ MetaDataConstants.METADATA_ALL_COLUMNS;
				while (hdfsHandler.hdfsFileExits(hdfspath.toLowerCase() + "/"
						+ metaDataFileName, hadoopProperties.get(Constants.HDFS_URL))) {
					version = Integer.toString((Integer.parseInt(version) + 1));
					metaDataFileName = country
							+ MetaDataConstants.METADATA_ALL_COLUMNS + "_V"
							+ version;
					hdfspath = metadataHDFSPath + "/" + metadataDBNAME + "/"
							+ srcSystem.toLowerCase()
							+ MetaDataConstants.METADATA_ALL_COLUMNS;
				} 

				log.info(srcSystem + ":" + country + ":"
						+ "writing AllTables record to hdfs in file" + hdfspath
						+ "/" + metaDataFileName);
			}

			result = hdfsHandler.hdfsWrite(metaDataString, propertiesMap,
					metaDataFileName, hdfspath.toLowerCase());

		} catch (IOException e) {
			System.err
			.println("Method: WriteToHdfs(). Class: MetaDataLoad.java. "
					+ "Output cannot be written to "
					+ metaDataFileName
					+ ".");
		}

		catch (Exception e) {
			log.info(e.getMessage());

		}
		log.info(hadoopProperties.get(Constants.ZOOKEEPER_PORT));
//		log.info(hadoopProperties.get(Constants.ZOOKEEPER_ZNODE));

		
	}

}
