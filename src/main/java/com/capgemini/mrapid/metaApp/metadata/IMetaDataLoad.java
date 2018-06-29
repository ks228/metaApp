/**
* The IMetaDataLoad interface for generator method declaration
* @author Pallavi Kadam 
*/

package com.capgemini.mrapid.metaApp.metadata;

import java.sql.SQLException;

public interface IMetaDataLoad {
	
	/**
	 * Method for loading metadata
	 * @param srcSystem
	 * @param country
	 * @param configPath
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws NumberFormatException 
	 */
	public boolean DataLoad(String srcSystem, String country, String configPath) throws NumberFormatException, ClassNotFoundException, SQLException;
}
