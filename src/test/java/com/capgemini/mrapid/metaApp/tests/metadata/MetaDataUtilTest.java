package com.capgemini.mrapid.metaApp.tests.metadata;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Test;

import com.capgemini.mrapid.metaApp.metadata.impl.MetaDataUtils;
import com.capgemini.mrapid.metaApp.tests.generator.GeneratorTest;

/**
 * @author Anuradha Dede
 *
 */
public class MetaDataUtilTest {

	@Test
	public void MetaDataJsonFileIsValidTest() throws IOException {
		MetaDataUtils loadTables=new MetaDataUtils();
		String jsonstr =loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE.json") ;
		boolean result=true;
			if(!(jsonstr.contains("DestinationSchema")&&jsonstr.contains("precision")&&jsonstr.contains("nullable")&&jsonstr.contains("length") &&jsonstr.contains("PrimaryKeyPosition")&&jsonstr.contains("columns")))
			{
				 result=false;
			}
		assertTrue(result);
	}
	
	@Test
	public void MetaDataJsonFileNotPresentTest()  {
		MetaDataUtils loadTables=new MetaDataUtils();
		boolean result=true;
		String jsonstr;
		try {
			jsonstr = loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE1.json");
			
			if(!(jsonstr.contains("DestinationSchema")&&jsonstr.contains("precision")&&jsonstr.contains("nullable")&&jsonstr.contains("length") &&jsonstr.contains("PrimaryKeyPosition")&&jsonstr.contains("columns")))
			{
				 result=false;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result=true;
			System.out.println("check for  MetaDataJsonFileNotPresent  test throws "+e.getLocalizedMessage());
		}
		
		assertTrue(result);
	}
	
	@Test
	public void MetaDataJsonFileNotValidTest()  {
		MetaDataUtils loadTables=new MetaDataUtils();
		boolean result=true;
		String jsonstr;
		try {
			jsonstr = loadTables.readFile(GeneratorTest.absolutePath+"/SOURCE1_ALL_MADVMODE.json");
			
			JSONObject MetaDataJson=new JSONObject(jsonstr);
//			System.out.println("MetaDataJson"+MetaDataJson);
			MetaDataJson.append("[", "]");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			result=true;
			System.out.println("check for  MetaDataJsonFileNotValidTest  test throws "+e.getLocalizedMessage());
		}
		
		assertTrue(result);
	}
}
