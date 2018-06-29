/**
 * 
 */
package com.capgemini.mrapid.metaApp.integration.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import com.capgemini.mrapid.metaApp.constants.Constants;


/**
 * @author audasi
 *
 */
public class MysqlHandler {

	Connection connection = null;
	String connectionUrl;
	String connectionUser;
	String connectionPassword;
	String connectionDriver;
	
	public MysqlHandler(Map<String,String> sourceProperty,String dbString,String password) {
	
		this.connectionUrl = sourceProperty.get(Constants.MYSQL_CONNECTION_URL) + dbString;
		this.connectionUser = sourceProperty.get(Constants.MYSQL_USER);
		this.connectionPassword = password;
		this.connectionDriver = sourceProperty.get(Constants.MYSQL_DRIVER_CLASS);
	}
	
	public Connection getConnection () {
		if (connection != null) {
			return connection;
		}
		try{
			Class.forName(connectionDriver).newInstance();
			connection = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
			return connection; 
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	public boolean closeConnection(Connection conn){
		
		try{
			if (conn != null) {
				conn.close();
				connection = null;
			}			
			return true;
		}catch(Exception e){
			System.out.println(e.getMessage());
			return false;
		}		
	}
	
}
