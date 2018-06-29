package com.capgemini.mrapid.metaApp.tests.generator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;

/**
* The GeneratorTest test cases to check valid configurations files
* @author  Anuradha Dede 
*/
public class GeneratorTest {
	
	public static Map<String, String> passedValue = new HashMap<String, String>();
	public static Map<String, String> propertiesMap = new HashMap<String, String>();
	Map<String, TableListStructure> tableListMap;
	ConfigUtils config = new ConfigUtils();
	Properties property = config.getConfigValues();
	String srcSystem = property.getProperty("SRCSYSTEM");
	String country =  property.getProperty("COUNTRY");
	public static String absolutePath;
	
	static{
		File file = new File("src/test/resources/");
		 absolutePath = file.getAbsolutePath();
	}
	
	/**
	 * Method to check the passed parameter are correct or not
	 */

	/**
	 * Method to verify configurations files path in config.properties are exists or not
	 */
	@Test
	public void propertiesFileExists() {

		boolean hadoopProFile = false, tablelist = false, sysProFile = false;
 
		File tableListFile = new File(absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		File hadoopConfPropertyFile = new File(absolutePath+"/"+property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		File systemPropertyFile = new File(absolutePath+"/"+property.getProperty( srcSystem + Constants.PROPERTIES_PATH));

		hadoopProFile = hadoopConfPropertyFile.exists();
		tablelist = tableListFile.exists();
		sysProFile = systemPropertyFile.exists();
		if ( tablelist == false) {
			System.err.println("test case failed "
					+ property.getProperty( srcSystem
							+ Constants.TABLE_LIST_PATH) + "path is not valid");
		}
		if (hadoopProFile == false) {
			System.err.println("test case failed "
					+ property.getProperty(srcSystem
							+ Constants.HADOOP_CONFIGURATION_PATH)
					+ "path is not valid");
		}
		
		assertTrue(hadoopProFile && tablelist && sysProFile);

	}

	/**
	 * Method to check table list map is not nullable
	 */
	@Test
	public void IstablelistNOTNULLTest() {

		Map<String, TableListStructure> tableListMap = config.
				readTableList(absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		assertNotNull(tableListMap);
	}

	
	/**
	 * Method to check properties map is not nullable
	 */
	@Test
	public void IsAPPPropertyNOTNULLTEST() {
		Map<String, TableListStructure> tableListMap = config
				.readTableList(absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));

		Map<String, String> propertiesMap = new HashMap<String, String>();
		Map<String, String> hadoopPropertiesMap = config.readPropertyFile(srcSystem,country,absolutePath+"/"+	property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		Map<String, String> sourcePropertiesMap =  config.readPropertyFile(srcSystem,country,absolutePath+"/" + property.getProperty(srcSystem + Constants.PROPERTIES_PATH));
		Map<String,String> countryPropertiesMap = config.readPropertyFile(srcSystem,country,absolutePath+"/" + property.getProperty(srcSystem + "_" + country + Constants.COUNTRY_PROPERTY_PATH));


		for (Map.Entry<String, TableListStructure> table : tableListMap
				.entrySet()) {
			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(), srcSystem, country,"");
		}
		assertNotNull(propertiesMap);

	}

}
