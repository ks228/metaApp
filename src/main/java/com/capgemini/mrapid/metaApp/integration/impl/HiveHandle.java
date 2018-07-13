package com.capgemini.mrapid.metaApp.integration.impl;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.constants.MetaDataConstants;
import com.capgemini.mrapid.metaApp.constants.RemedyConstants;
import com.capgemini.mrapid.metaApp.exceptions.HiveConnectionExcetion;
import com.capgemini.mrapid.metaApp.integration.api.IIntegrate;
import com.capgemini.mrapid.metaApp.utils.ParserUtils;
import com.capgemini.mrapid.metaApp.utils.PwdDecryptor;
import com.capgemini.mrapid.metaApp.utils.RemedyLogsUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Class HDFSHandle : Hive utilities
 * @author Anuradha Dede
 *
 */
public class HiveHandle implements IIntegrate {
	final Logger log = Logger.getLogger(HiveHandle.class);

	//private static String driverName = "com.cloudera.hive.jdbc4.HS2Driver"; //TODO - has to be property driven
	private static String driverName = "org.apache.hive.jdbc.HiveDriver"; //TODO - has to be property driven
	
	private static String hiveUser = "";
	private static String hivePwd = "";
	private static String hiveInputFormat = "";
	private static String hive_txn_manager = "";
	private static String hive_enforce_bucketing = "";
	private static String hive_exec_dynamic_partition_mode = "";
	private static String hive_txn_timeout = "";
	private static String hive_support_concurrency = "";
	private static String hive_compactor_initiator_on = "";
	private static String hive_compactor_worker_threads = "";
	private static String datanucleus_connectionpoolingtype = "";

	/**
	 * getHandle:Gets Hive configuration object
	 * 
	 * @param hdfsUrl
	 *            :HDFS IP with port as string
	 * @return Hive Configuration object
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getHandle(String hiveUrl) throws ClassNotFoundException,
			SQLException {
		// TODO Auto-generated method stub
		Connection con = null;
		try {
			Class.forName(driverName);
			con = DriverManager.getConnection(hiveUrl, hiveUser, hivePwd);
//			stmt = con.createStatement();
//			stmt.execute(hiveInputFormat);
//			stmt.execute(hive_txn_manager);
//			stmt.execute(hive_enforce_bucketing);
//			stmt.execute(hive_exec_dynamic_partition_mode);
//			stmt.execute(hive_txn_timeout);
//			stmt.execute(hive_support_concurrency);
//			stmt.execute(hive_compactor_initiator_on);
//			stmt.execute(hive_compactor_worker_threads);
//			stmt.execute(datanucleus_connectionpoolingtype);

		} finally {
			// con.close();
		}
		return con;
	}

	/**
	 * insert: insert into Hive Table
	 * @param query
	 * @param hadoopProperties
	 * @param hdfsURL
	 * @param srcSystem
	 * @param country
	 * @return string - sucess/failure
	 * @throws SQLException
	 */
	public String insert(String query, Map<String, String> hadoopProperties,String hdfsURL,String srcSystem ,String country)
			throws SQLException {
		Statement stmt = null;
		Connection con = null;
		try {
			setHiveProperty(hadoopProperties,hdfsURL,srcSystem,country);
			con = getHandle(hadoopProperties.get(Constants.HIVE_CONNECTION_URL));
			if (!(con == null)) {
				stmt = con.createStatement();
				boolean res = stmt.execute(query);
				if (res) {
					log.error("unable to insert check query or hive connection url"
							+ res);
				}
			}
		} catch (ClassNotFoundException e) {
			log.error("unable to insert check query or hive connection url"
					+ e.getMessage());
		} finally {
			con.close();
		}

		return "sucess";

	}

	/**
	 * select: select from Hive Table
	 * @param query : String object having insert query
	 * @param hive_URL
	 * @return String success or failures
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String select(String query, String hive_URL) throws SQLException,
			ClassNotFoundException {
		String maxversion = "";
		Statement stmt = null;
		ResultSet res = null;
		Connection con = null;
		con = getHandle(hive_URL);
		try {
			if (!(con == null)) {
				stmt = con.createStatement();
				res = stmt.executeQuery(query);
				if (!(res == null)) {
					if (res.next())
						maxversion = res.getString("version");
				} else {
					log.error("No results found check the Query need to execute"
							+ query);
				}
			} else {
				log.error("Unable to connect to hive" + hive_URL);
			}

		} finally {
			res.close();
			con.close();
		}

		return maxversion;
	}

	/**
	 * Get max version of table for given srcSystem and country
	 * @param hadoopProperties
	 * @param query
	 * @param country
	 * @param srcSystem
	 * @param hdfsURL
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String selectMaxVersion(Map<String, String> hadoopProperties,
			String query, String country, String srcSystem,String hdfsURL) throws SQLException,
			ClassNotFoundException {
		String maxversion = "";
		ResultSet res = null;
		Connection con = null;
		
		log.info(srcSystem + ":" + country + ":" + "Connectiong to hive"
				+ "hive_URL::"
				+ hadoopProperties.get(Constants.HIVE_CONNECTION_URL));
		setHiveProperty(hadoopProperties,hdfsURL,srcSystem,country);
		con = getHandle(hadoopProperties.get(Constants.HIVE_CONNECTION_URL));

		try {
			if (!(con == null)) {

				log.info("fetching version from table");
				PreparedStatement pstmt = con.prepareStatement(query);
				pstmt.setString(1, srcSystem);
				pstmt.setString(2, country);
		    	res = pstmt.executeQuery();
				if (!(res == null)) {
					while (res.next()) {
						if (res.getString(1) != null)
							maxversion = res.getString(1);
						else
							maxversion = "0";
					}

				} else {
					log.error("No results found check the Query need to execute"
							+ query);
				}
			} else {
				maxversion = MetaDataConstants.CONNECTION_ERROR;
				log.error(srcSystem + ":" + country + ":"
						+ "Failed to connect to hive so exiting" + "hive_URL::"
						+ hadoopProperties.get(Constants.HIVE_CONNECTION_URL));
			}

		} finally {
			con.close();
			res.close();
		}

		return maxversion;
	}
	
	/**
	 * Get max version and table list for given srcSystem and country
	 * @param hadoopProperties
	 * @param query
	 * @param parameters
	 * @param hdfsURL
	 * @param srcSystem
	 * @param country
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public Map<String,Integer> selectMaxVersionTableMap(Map<String, String> hadoopProperties,String query,List<String> parameters,String srcSystem,String country,Map<String,String> deploymentProperties) throws SQLException,
			ClassNotFoundException {
		
		Map<String,Integer> maxversion = new HashMap<String,Integer>();
		setHiveProperty(hadoopProperties,hadoopProperties.get(Constants.HDFS_URL),srcSystem,country);
		ResultSet res = null;
		Connection con = null;
		con = getHandle(hadoopProperties.get(Constants.HIVE_CONNECTION_URL));
		try {
			if (!(con == null)) {
				PreparedStatement pstmt = con.prepareStatement(query);			
						
				for (int i = 0; i < parameters.size(); i++) {
					pstmt.setString((i+1), parameters.get(i));
				}
				res = pstmt.executeQuery();
				if (res != null) {
					log.info("fetching version from table");
					while (res.next()) {
						String tableName = ParserUtils.removeControlChar(res.getString(1));
						Integer tableVersion = Integer.parseInt(res.getString(2))+1;
						maxversion.put(tableName, tableVersion);
					}
				}else {
					log.error("No results found check the Query need to execute" + query);
					throw new HiveConnectionExcetion("No results found check the Query need to execute");
				}
			}else {
				log.error(srcSystem + ":" + country + ":" + "Failed to connect to hive so exiting" + "hive_URL::" + hadoopProperties.get(Constants.HIVE_CONNECTION_URL));
				throw new HiveConnectionExcetion("Unable to connect to hive");
			}

		}catch(HiveConnectionExcetion e){
			log.error("Not able to established Hive connection / Check common tables" + e.getMessage());
			RemedyLogsUtils.writeToRemedyLogs(RemedyConstants.METAAPP_COMPONENT,srcSystem,country,RemedyConstants.PRODUCT_CODE_TWENTYFIVE,
					RemedyLogsUtils.remedyLogsPropertiesMap.get(RemedyConstants.PRODUCT_CODE_TWENTYFIVE),"Hive connection problem / Check common tables",deploymentProperties);
			RemedyLogsUtils.closeRemedyLogFileHandle();
			System.exit(1);
		}finally {
			con.close();
			res.close();
		}
		return maxversion;
	}
	
	/**
	 * Update method to update existing table schema
	 * @param hive_URL
	 * @param updateQuery
	 * @param country
	 * @param srcSystem
	 * @param allTableTablename
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public String updateParam(String hive_URL,String updateQuery, String country,
			String srcSystem, String allTableTablename) throws SQLException, ClassNotFoundException{
			int resultSet;
			Connection con = null;
			log.info("Performing Updation " + srcSystem + ":" + country + ":" + "Connectiong to hive"
					+ "hive_URL::" + hive_URL);
			con = getHandle(hive_URL);
			try 
			{
					if (!(con == null)) 
					{
						log.info(srcSystem + ":" + country + ":"
								+ "Executing Update query" + updateQuery);
						PreparedStatement pstmt = con.prepareStatement(updateQuery);
						if(updateQuery.contains("_all_countries"))
						{
							pstmt.setString(1, srcSystem);
							pstmt.setString(2, country);
						}
						else if(updateQuery.contains("_all_tables"))
						{
							pstmt.setString(1, country);
							pstmt.setString(2, srcSystem);
							pstmt.setString(3, allTableTablename);
						}
						resultSet = pstmt.executeUpdate();
						if (resultSet != 0) 
						{
							log.info(srcSystem + ":" + country + ":"
									+ updateQuery +" number of rows updated");
							return "success";
					
						} 
						else 
						{
							log.error("No rows updated with query :"
									+ updateQuery);
							return "success"; 
						}
					} 
					else 
					{
						log.error(srcSystem + ":" + country + ":"
						+ "Failed to connect to hive so exiting" + "hive_URL::"
						+ hive_URL);
						return "failure";
					}

			} finally 
				{
				con.close();
				}
			}	
	
	/**
	 * setHiveProperty: Set Hive 
	 * @param query : String object having insert query
	 * @return String success or failures
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void setHiveProperty(Map<String, String> hadoopProperties,String hdfsURL,String srcSystem,String country)  {
	
//		HDFSHandle hdfshandler= new HDFSHandle();
//		String pwd=hdfshandler.hdfsRead(hadoopProperties.get(Constants.HIVE_CONNECTION_PWD), hdfsURL, srcSystem, country,
//				MetaDataConstants.TABLENAME_HIVEPWD);
		
//		hivePwd = pwd;
		try{
			hivePwd = new PwdDecryptor().decrypt(hadoopProperties.get(Constants.HIVE_CONNECTION_PWD));
//		hivePwd = hadoopProperties.get(Constants.HIVE_CONNECTION_PWD);
		hiveUser = hadoopProperties.get(Constants.HIVE_USER_NAME);
		hiveInputFormat = MetaDataConstants.HIVE_INPUT_FORMAT
				+ hadoopProperties.get(Constants.HIVE_INPUT_FORMAT_PROPERTY);
		hive_txn_manager = MetaDataConstants.HIVE_TXN_MANAGER
				+ hadoopProperties.get(Constants.HIVE_TXN_MANAGER_PROPERTY);
		hive_txn_timeout = MetaDataConstants.HIVE_TXN_TIMEOUT
				+ hadoopProperties.get(Constants.HIVE_TXN_TIMEOUT_PROPERTY);
		hive_compactor_initiator_on = MetaDataConstants.HIVE_COMPACTOR_INITIATO_ON
				+ hadoopProperties
						.get(Constants.HIVE_COMPACTOR_INITIATO_ON_PROPERTY);
		hive_compactor_worker_threads = MetaDataConstants.HIVE_COMPACTOR_WORKER_THREADS
				+ hadoopProperties
						.get(Constants.HIVE_COMPACTOR_WORKER_THREADS_PROPERTY);
		datanucleus_connectionpoolingtype = MetaDataConstants.DATANUCLEUS_CONNECTIONPOOLINGTYPE
				+ hadoopProperties
						.get(Constants.DATANUCLEUS_CONNECTIONPOOLINGTYPE_PROPERTY);
		hive_enforce_bucketing = MetaDataConstants.HIVE_ENFORCE_BUCKETING
				+ hadoopProperties
						.get(Constants.HIVE_ENFORCE_BUCKETING_PROPERTY);
		hive_support_concurrency = MetaDataConstants.HIVE_SUPPORT_CONCURRENCY
				+ hadoopProperties
						.get(Constants.HIVE_SUPPORT_CONCURRENCY_PROPERTY);
		hive_exec_dynamic_partition_mode = MetaDataConstants.HIVE_EXEC_DYNAMIC_PARTITION_MODE
				+ hadoopProperties
						.get(Constants.HIVE_EXEC_DYNAMIC_PARTITION_MODE_PROPERTY);
		}catch(Exception e){
			log.error("Password decrytor failed "+e);
			e.printStackTrace();
		}
	}

}
