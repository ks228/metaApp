/**
* The IGenerator interface for generator method declaration
* @author  Anurag Udasi 
*/
package com.capgemini.mrapid.metaApp.schemagenerator;

public interface  IGenerator {

	/**
	 * Method for creating, parsing json and generating hql,avsc and avro file
	 * @param srcSystem
	 * @param country
	 * @return
	 */
	public boolean generate(String srcSystem,String country,String configPath);
}
