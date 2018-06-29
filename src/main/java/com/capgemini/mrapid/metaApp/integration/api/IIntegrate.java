/**
 * The IIntegrate interface for getHandle method declaration
 * @author Anuradha Dede
*/

package com.capgemini.mrapid.metaApp.integration.api;

import java.sql.SQLException;

public interface IIntegrate {
	
	 /**
	 * declaration of method to get HDFS File System object 
	 * @param source
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public Object getHandle(String source) throws ClassNotFoundException, SQLException;

}
