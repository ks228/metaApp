/**
 * IParser is Interface declaring createJsonfromDDL method
 *  @author Anurag Udasi
 *
 */
package com.capgemini.mrapid.metaApp.metaparser;

import java.util.ArrayList;
import java.util.Map;

import org.apache.poi.ss.usermodel.Workbook;
import org.json.JSONObject;
public interface IParser {
	
	/**
	 * 	Declaration of method to generate JSON from schema file
	 * @param schema
	 * @param metaInformation
	 * @param propertiesMap
	 * @param srcSystem
	 * @param country
	 * @param table
	 * @param datatypeMap
	 * @param workbook
	 * @param excelPropertyMap
	 * @param tableClassification
	 * @return
	 */
	public JSONObject createJsonfromSchema(ArrayList<String> schema, JSONObject metaInformation, Map<String, String> propertiesMap,
			String srcSystem, String country, String table, Map<String,String> datatypeMap,Workbook workbook,Map<String,String> excelPropertyMap,String tableClassification);
}
