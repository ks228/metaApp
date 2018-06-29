package com.capgemini.mrapid.metaApp.tests.parser;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.*;

/**
* The CSVtoJsonConverterTest test cases of CSV to Json Converter 
* @author  Anuradha Dede 
*/

public class CSVtoJsonConverterTest {

	public static Map<String, String> propertiesMap = new HashMap<String, String>();
	Map<String, TableListStructure> tableListMap;
	ConfigUtils conFig = new ConfigUtils();
	Properties property = conFig.getConfigValues();
	String srcSystem = property.getProperty("SRCSYSTEM");
	String country =  property.getProperty("COUNTRY");

	/**
	 * Method to check metaJson created is valid or not  
	 */
	@Test
	public void metaJsonValidTest() {
		boolean result = false;			
		ConfigUtils config = new ConfigUtils();
		Properties property = config.getConfigValues();
		Map<String, TableListStructure> tableListMap = config
						.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		Map<String, String> hadoopPropertiesMap = config.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+
						property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		Map<String, String> sourcePropertiesMap =  config.
					readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.PROPERTIES_PATH));
		Map<String,String> countryPropertiesMap = config.
					readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + country +  Constants.COUNTRY_PROPERTY_PATH));
		
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {

			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(), srcSystem, country,"");
			JSONObject metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap, propertiesMap, table.getValue(), country);
			result = JsonUtils.isValidJson(metaInformation);
		}
		assertTrue(result);
	}
	
	/**
	 * Method to check metaJson created is handled or not  
	 */
	@Test
	public void metaJsonINvalidHandleTest() {
		boolean result = false;			
		ConfigUtils config = new ConfigUtils();
		Properties property = config.getConfigValues();
		Map<String, TableListStructure> tableListMap = config
						.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		Map<String, String> hadoopPropertiesMap = config.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+
						property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		Map<String, String> sourcePropertiesMap =  config.
					readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.PROPERTIES_PATH));
		Map<String,String> countryPropertiesMap = config.
					readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + country +  Constants.COUNTRY_PROPERTY_PATH));
		
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {

			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(), srcSystem, country,"");
			JSONObject metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap, propertiesMap, table.getValue(), country);
			try {
				metaInformation.append("{", "}");
				metaInformation.get("SchemaInfo");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				result=true;
				System.out.println("check for metaJsonINvalid  test throws "+e.getLocalizedMessage());
			}
			
		}
		assertTrue(result);
	}
}

