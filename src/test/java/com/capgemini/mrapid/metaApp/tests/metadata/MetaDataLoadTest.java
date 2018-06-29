package com.capgemini.mrapid.metaApp.tests.metadata;

import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.junit.Test;

import com.capgemini.mrapid.metaApp.constants.Constants;
import com.capgemini.mrapid.metaApp.exceptions.MetaDataProcessingError;
import com.capgemini.mrapid.metaApp.metadata.impl.MetaDataUtils;
import com.capgemini.mrapid.metaApp.pojo.TableListStructure;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;
import com.capgemini.mrapid.metaApp.utils.ConfigUtils;

/**
 * @author Anuradha Dede
 *
 */
public class MetaDataLoadTest {
	
	ConfigUtils config = new ConfigUtils();
	Properties property = config.getConfigValues();
	String srcSystem = property.getProperty("SRCSYSTEM");
	String country = property.getProperty("COUNTRY");

	
	@Test
	public void allCountryStringIsValidTest() {
		
		ConfigUtils config = new ConfigUtils();
		boolean result=true;
		MetaDataUtils metaload = new MetaDataUtils();
		
		Map<String, String> metaDataDefaultMap = config.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+ property.getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
		
		String allcountryversion = metaDataDefaultMap.get("VERSION");
		
		String allCntry = metaload.getCountryMeta(srcSystem, country, allcountryversion,metaDataDefaultMap,"false");
		String allcntry[]=allCntry.split(Pattern.quote(metaload.getDelimiter(srcSystem,country,metaDataDefaultMap)));
		if(!(allcntry.length==14)){
			result=false;
		}

		assertTrue(result);
	}
	
	@Test
	public void allTablesStringIsValidTest() {
		
		boolean result=true;
		String thresholdVal = "99p";
		MetaDataUtils metaload = new MetaDataUtils();

		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		
		Map<String,String> hadoopProperties = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		
		Map<String,String> metaDataDefaultMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property .getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
				
		String version = metaDataDefaultMap.get("VERSION");
		String metadataHDFSURL = hadoopProperties.get(Constants.HDFS_URL);
		
		String[] hdfsserver = metadataHDFSURL.split(":");
		String server = hdfsserver[0];
		int port = Integer.parseInt(hdfsserver[1]);
		ArrayList<String> all_tablesArr = new ArrayList<String>();
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			try {
				String alltable = metaload.getTableMeta(srcSystem, country, table.getValue().getName(), table.getValue().getClassification(),thresholdVal, version, server, port,metaDataDefaultMap,"false");
			
				all_tablesArr.add(alltable);
				
				String alltables[]=alltable.split(Pattern.quote(metaload.getDelimiter(srcSystem,country,metaDataDefaultMap)));
				if(!(alltables.length==18)){
					result=false;
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		assertTrue(result);
	}
	
	@Test
	public void allTabColStringIsValidTest() {
		
		boolean result=true;

		MetaDataUtils metaload = new MetaDataUtils();
		ArrayList<String> columnArr = new ArrayList<String>();
		Map<String, String> sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem
						+ Constants.PROPERTIES_PATH));

		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
				
		Map<String,String> metaDataDefaultMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
		
		String version = metaDataDefaultMap.get("VERSION");
		
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			
			try {
				String jsonstr =metaload.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE.json") ;
		
				columnArr = metaload.getTableColumsMeta(jsonstr, srcSystem, country,table.getValue().getName(), version.toString(),metaDataDefaultMap,sourcePropertiesMap,"false");
			
			  String alltables[]=null;

				   alltables= columnArr.get(0).split(Pattern.quote(metaload.getDelimiter(srcSystem,country,metaDataDefaultMap)));

				
				if(!(alltables.length==18)){
					result=false;
				}
			} catch (Exception e) {
				
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}
		}
		assertTrue(result);
	}
	

	@Test
	public void allCountryProcessingTest() {
		
		ConfigUtils config = new ConfigUtils();
		boolean result=true;
		MetaDataUtils metaload = new MetaDataUtils();
		
		Map<String, String> metaDataDefaultMap = config.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+ property.getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
		
		String allcountryversion = metaDataDefaultMap.get("VERSION");
		
		try {
			String allCntry = metaload.getCountryMeta(srcSystem, country, allcountryversion,metaDataDefaultMap,"false");
			String allcntry[]=allCntry.split(",");
			
			if(!(allcntry.length==14)){
				throw new MetaDataProcessingError("allCountry Processing fails");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("check for negative test throws "+e.getLocalizedMessage());
			result =true;
//			e.printStackTrace();
		}

		assertTrue(result);
	}
	
	@Test
	public void allTablesProcessingTest() {
		
		boolean result=true;
		String thresholdVal = "99p";
		MetaDataUtils metaload = new MetaDataUtils();

		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
		
		Map<String,String> hadoopProperties = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + "_" + Constants.HADOOP_CONFIGURATION_PATH));
		
		Map<String,String> metaDataDefaultMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property .getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
				
		String version = metaDataDefaultMap.get("VERSION");
		String metadataHDFSURL = hadoopProperties.get(Constants.HDFS_URL);
		
		String[] hdfsserver = metadataHDFSURL.split(":");
		String server = hdfsserver[0];
		int port = Integer.parseInt(hdfsserver[1]);
		ArrayList<String> all_tablesArr = new ArrayList<String>();
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			try {
				String alltable = metaload.getTableMeta(srcSystem, country, table.getValue().getName(), table.getValue().getClassification(),thresholdVal ,version, server, port,metaDataDefaultMap,"false");			
				all_tablesArr.add(alltable);
				
				String alltables[]=alltable.split(",");
				if(!(alltables.length==18)){
					throw new MetaDataProcessingError("allTables Processing fails");
				}
			} catch (ParseException e) {
			
			} catch (MetaDataProcessingError e) {
				// TODO Auto-generated catch block
				System.out.println("check for negative test throws "+e.getLocalizedMessage());
				result =true;

			}
		}
		assertTrue(result);
	}
	
	@Test
	public void allTabColProcessinTest() {
		
		boolean result=true;

		MetaDataUtils metaload = new MetaDataUtils();
		ArrayList<String> columnArr = new ArrayList<String>();
		Map<String, String> sourcePropertiesMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem
						+ Constants.PROPERTIES_PATH));

		Map<String, TableListStructure> tableListMap = config
				.readTableList(GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem + Constants.TABLE_LIST_PATH));
				
		Map<String,String> metaDataDefaultMap = config
				.readPropertyFile(srcSystem,country,GeneratorTest.absolutePath+"/"+property.getProperty(srcSystem+Constants.METADATA_PROPERTIES_PATH));
		
		String version = metaDataDefaultMap.get("VERSION");
		
		for (Map.Entry<String, TableListStructure> table : tableListMap.entrySet()) {
			
			try {
				String jsonstr =metaload.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE.json") ;
		
				columnArr = metaload.getTableColumsMeta(jsonstr, srcSystem, country,table.getValue().getName(), version.toString(),metaDataDefaultMap,sourcePropertiesMap,"false");
			
			  String alltables[]=null;

				   alltables= columnArr.get(0).split(",");

				
				if(!(alltables.length==18)){
					throw new MetaDataProcessingError("allTables Processing fails");
				}
			} catch (Exception e) {
				System.out.println("check for negative test throws "+e.getLocalizedMessage());
				result =true;
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}
		}
		assertTrue(result);
	}
	
	
}