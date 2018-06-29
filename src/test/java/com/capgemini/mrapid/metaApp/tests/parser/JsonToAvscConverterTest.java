package com.capgemini.mrapid.metaApp.tests.parser;

import static org.junit.Assert.assertTrue;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.FileReaderFactory;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.ParserFactory;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.CommonUtils;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;
import com.capgemini.mrapid.metaApp.utils.MetaInfoGenerator;

/**
 * The JsonToAvscConverterTest test cases of Avsc from json is valid or not
 * @author Anuradha Dede
 */
public class JsonToAvscConverterTest {

	public static Map<String, String> propertiesMap = new HashMap<String, String>();
	Map<String, TableListStructure> tableListMap;
	ConfigUtils config = new ConfigUtils();
	Properties property = config.getConfigValues();
	String srcSystem = property.getProperty("SRCSYSTEM");
	String country =  property.getProperty("COUNTRY");
	Workbook workbook = null;
	


	/**
	 * Method to check Json created is valid or not
	 */
	@Test
	public void isCreatedJsonValidTest() {
		boolean result = false;
		
		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));

		Map<String, String> hadoopPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));

		Map<String, String> sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.PROPERTIES_PATH));
		
		Map<String,String> countryPropertiesMap = config.
				readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + country +  Constants.COUNTRY_PROPERTY_PATH));
	
		Map<String, String> dataTypeMappingPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + "_" + country + Constants.DATATYPE_MAPPING_PATH));


		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			
			String tableName = table.getValue().getName();
			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(), srcSystem, country,"");
			String ddlschemaFileName = GeneratorTest.absolutePath + propertiesMap.get(Constants.SCHEMA_DIR) + "/" + srcSystem + "_" + country + "_" + table.getValue().getName() + Constants.SQL_FORMAT;			
			
			System.out.println("ddlschemaFileName"+ddlschemaFileName);
			// parse meta information
			JSONObject metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap, propertiesMap, table.getValue(), country);
			
			// Read SQL schema file from location specified
			FileReader ddlschemaFile = CommonUtils.readFile(ddlschemaFileName);
			
			String inputDataType = sourcePropertiesMap.get(Constants.SOURCE_INPUT_TYPE);

			if (ddlschemaFile != null) {

				ISchemaReader schemaFileReader = FileReaderFactory.createSchemaReader(inputDataType);
				ArrayList<String> ddlschemaFileList = schemaFileReader.scanSchemaFile(ddlschemaFile);

				if (ddlschemaFileList.size() != 0) {

					String classification = "Master";
					JSONObject schemaInformationObject = GenerateJSON(inputDataType, srcSystem, country, tableName, ddlschemaFileList, metaInformation, dataTypeMappingPropertiesMap,workbook,classification);

					if (schemaInformationObject.has("MetaInformation")
							&& schemaInformationObject.has("DestinationSchema")
							&& schemaInformationObject.has("SourceSchema")) {
						result = true;
					}

				}
			}

		}

		assertTrue(result);
	}
	
	/**
	 * Method to check Json created is valid or not
	 */
	@Test
	public void JsonInValidTest() {
		boolean result = false;
		
		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));

		Map<String, String> hadoopPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));

		Map<String, String> sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + Constants.PROPERTIES_PATH));
		
		Map<String,String> countryPropertiesMap = config.
				readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + country +  Constants.COUNTRY_PROPERTY_PATH));
	
		Map<String, String> dataTypeMappingPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath + "/" + property.getProperty(srcSystem + "_" + country + Constants.DATATYPE_MAPPING_PATH));


		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			
			String tableName = table.getValue().getName();
			propertiesMap = config.mapProperty(countryPropertiesMap,sourcePropertiesMap,hadoopPropertiesMap, table.getValue().getName(), srcSystem, country,"");
			String ddlschemaFileName = GeneratorTest.absolutePath + propertiesMap.get(Constants.SCHEMA_DIR) + "/" + srcSystem + "_" + country + "_" + table.getValue().getName() + Constants.SQL_FORMAT;			
			
			// parse meta information
			JSONObject metaInformation = MetaInfoGenerator.getMetaInfoJsonObject(countryPropertiesMap, sourcePropertiesMap, propertiesMap, table.getValue(), country);
			
			// Read SQL schema file from location specified
			FileReader ddlschemaFile = CommonUtils.readFile(ddlschemaFileName);
			
			String inputDataType = sourcePropertiesMap.get(Constants.SOURCE_INPUT_TYPE);

			if (ddlschemaFile != null) {

				ISchemaReader schemaFileReader = FileReaderFactory.createSchemaReader(inputDataType);
				ArrayList<String> ddlschemaFileList = schemaFileReader.scanSchemaFile(ddlschemaFile);

				if (ddlschemaFileList.size() != 0) {
					String classification = "Master";
					JSONObject schemaInformationObject = GenerateJSON(inputDataType, srcSystem, country, tableName, ddlschemaFileList, metaInformation, dataTypeMappingPropertiesMap,workbook,classification);
					try {
						schemaInformationObject.append("{", "}");
//						result = JsonUtils.isValidJson(schemaInformationObject);
						schemaInformationObject.get("MetaInfo");
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						result=true;
						System.out.println("check for created JsonInValidTest  test throws "+e.getLocalizedMessage());
//						e.printStackTrace();
					}
					
					

				}
			}

		}

		assertTrue(result);
	}


	/**
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param ddlschemaFileList
	 * @param metaInformation
	 * @param datatypeMap
	 * @return
	 */
	private JSONObject GenerateJSON(String inputDataType, String srcSystem, String country, String table, ArrayList<String> ddlschemaFileList, JSONObject metaInformation, Map<String, String> datatypeMap,Workbook workbook,String classification) {
		IParser Parser = ParserFactory.createParser(inputDataType);
		JSONObject schemaInformationObject = Parser.createJsonfromSchema( ddlschemaFileList, metaInformation, propertiesMap, srcSystem, country, table, datatypeMap,workbook,null,classification);

		return schemaInformationObject;
	}

}
