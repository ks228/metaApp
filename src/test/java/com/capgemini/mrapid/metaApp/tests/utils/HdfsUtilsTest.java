package com.capgemini.mrapid.metaApp.tests.utils;

import static org.junit.Assert.assertTrue;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONObject;
import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.exceptions.HDFSFileOperationException;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.metaparser.IParser;
import com.capgemini.mrapid.metaApp.metaparser.ISchemaReader;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.FileReaderFactory;
import com.capgemini.mrapid.metaApp.schemagenerator.impl.ParserFactory;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.*;

/**
 * @author Anuradha Dede
 */
public class HdfsUtilsTest extends Configured  {

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

	/**
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param ddlschemaFileList
	 * @param metaInformation
	 * @param datatypeMap
	 * @return
	 */
	private JSONObject GenerateJSON(String inputDataType, String srcSystem, String country, String table, ArrayList<String> ddlschemaFileList, JSONObject metaInformation, Map<String, String> datatypeMap,Workbook workbook, String classification) {
		IParser Parser = ParserFactory.createParser(inputDataType);
		JSONObject schemaInformationObject = Parser.createJsonfromSchema( ddlschemaFileList, metaInformation, propertiesMap, srcSystem, country, table, datatypeMap,workbook,null,classification);

		return schemaInformationObject;
	}

	
}

