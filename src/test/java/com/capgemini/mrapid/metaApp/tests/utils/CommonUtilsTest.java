package com.capgemini.mrapid.metaApp.tests.utils;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.*;

/**
* The CommonUtilsTest test cases check common Utility functionality
* @author  Anuradha Dede 
*/
public class CommonUtilsTest {

	public static Map<String, String> propertiesMap = new HashMap<String, String>();
	Map<String, TableListStructure> tableListMap;
	ConfigUtils config = new ConfigUtils();
	Properties property = config.getConfigValues();
	String srcSystem = property.getProperty("SRCSYSTEM");
	String country =  property.getProperty("COUNTRY");


	/**
	 * Method to check metaDLLFile for tables are exists or not
	 */
	@Test
	public void metaDLLFilePathExistTest() {
		boolean ddlschemaFile = false;
		boolean metaInfoFile = false;
		
		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));

		Map<String, String> hadoopPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));

		Map<String, String> sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.PROPERTIES_PATH));

		Map<String,String> countryPropertiesMap = config.
				readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + country +  Constants.COUNTRY_PROPERTY_PATH));

		
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
		
			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(),srcSystem, country,"");

			String ddlschemaFileName = GeneratorTest.absolutePath + propertiesMap.get(Constants.SCHEMA_DIR) + "/" + srcSystem + "_" + country + "_" + table.getValue().getName() + Constants.SQL_FORMAT;
						
			String metaInfoFileName = GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_PROPERTIES_PATH");
						
			File metaInfoFilObj = new File(metaInfoFileName);
			File ddlschemaFileObj = new File(ddlschemaFileName);
			metaInfoFile = metaInfoFilObj.exists();
			ddlschemaFile = ddlschemaFileObj.exists();
			if (ddlschemaFile == false) {
				System.err.println(" TestCase Fail ddlschemaFile Path not exist");
			}
			if (metaInfoFile == false) {
				System.err.println(" TestCase Fail metaInfoFile Path not exist");
			}
		}
		assertTrue(ddlschemaFile && metaInfoFile);
	}
}
