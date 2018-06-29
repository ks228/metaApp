package com.capgemini.mrapid.metaApp.tests.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.integration.impl.HDFSHandle;
import com.capgemini.mrapid.metaApp.metadata.impl.MetaDataUtils;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;
import com.capgemini.mrapid.metaApp.utils.SchemaEvolution;

public class SchemaEvolutionTest {

	public static Map<String, String> propertiesMap = null;
	Map<String, TableListStructure> tableListMap = null;
	Map<String, String> sourcePropertiesMap = null;
	Map<String, String> countryPropertiesMap = null;
	Map<String, String> hadoopPropertiesMap = null;
	String srcSystem = "";
	String country = "";
	ConfigUtils config = null;
	Properties property = null;

	@Before
	public void setUp() {
		config = new ConfigUtils();
		property = config.getConfigValues();
		srcSystem = property.getProperty("SRCSYSTEM");
		country = property.getProperty("COUNTRY");

		tableListMap = config.readTableList(GeneratorTest.absolutePath + "/"
				+ property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		hadoopPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath
						+ "/"
						+ property.getProperty(srcSystem + "_"
								+ Constants.HADOOP_CONFIGURATION_PATH));
		sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath
						+ "/"
						+ property.getProperty(srcSystem
								+ Constants.PROPERTIES_PATH));
		countryPropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath
						+ "/"
						+ property.getProperty(srcSystem + "_" + country
								+ Constants.COUNTRY_PROPERTY_PATH));

	}

	@Test
	public void checkoldSourceSchemaFile() {
		boolean result = false;
		HDFSHandle hdfsHandleobj = new HDFSHandle();
		JSONObject jsonObj = null;

		for (Map.Entry<String, TableListStructure> table : tableListMap
				.entrySet()) {

			propertiesMap = config.mapProperty(countryPropertiesMap,
					sourcePropertiesMap, hadoopPropertiesMap, table.getValue()
							.getName(), srcSystem, country,"");

			try {
				String hdfsPath = propertiesMap.get(Constants.JSON_OUTPUT_DIR)
						+ "/" + srcSystem + "_" + country + "_"
						+ table.getValue().getName() + Constants.JSON_FORMAT;

				hdfsHandleobj.hdfsRead(hdfsPath.toLowerCase(),
						propertiesMap.get(Constants.HDFS_URL));

			} catch (Exception e) {
				// log.error(e.getMessage());
				// TODO Auto-generated catch block
				result=true;
//				e.printStackTrace();
			}

		}
		assertTrue(result);
	}

	
	
	@Test
	public void compareDiffSourceSchemaTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
		System.out.println(flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
		
	}
	
	@Test
	public void compareSameSourceSchemaTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
		System.out.println("smae source"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
		
	@Test
	public void compareSourceSchemaDatatypeTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_1.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	
	@Test
	public void compareSourceSchemapKTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_1.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	
	@Test
	public void compareSourceSchemaEmptyPKTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_nullpk.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	
	@Test
	public void compareSourceSchemaScaleTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_pricision.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	@Test
	public void compareSourceSchemaColNameTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_colName.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	
	@Test
	public void compareSourceSchemaDatabaseNameTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_DataBase.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("datatype"+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	@Test
	public void compareSourceSchemaDestChangeeTest() {
		boolean flag=false;
		try {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr1 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS.json") ;
		String jsonstr2 =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_USERS_destchange.json") ;
		JSONObject jsonObj = new JSONObject(jsonstr1.toString());
		JSONObject jsonObj1 = new JSONObject(jsonstr2.toString());

	
		 flag=	SchemaEvolution.compareSourceSchema(jsonObj, jsonObj1);
//		System.out.println("dest schema "+flag);

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!flag)
		{
			
			assertTrue(!flag);
			
		}
	}
	
	@After
	public void tearDown() {
		hadoopPropertiesMap = null;
		sourcePropertiesMap=null;
		countryPropertiesMap = null;
	}

}
